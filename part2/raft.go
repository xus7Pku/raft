package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DebugCM = 1

// 这是向客户端发送 commit 日志的通道元素，这里的客户端不是通常意义上的，而是以 raft 为基础的客户端（比如数据库集群）
type CommitEntry struct {
	Command interface{}
	Index   int
	Term    int
}

type CMState int

const (
	Follower CMState = iota
	Canditate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Canditate:
		return "Canditate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// 这是内部日志的保存格式
type LogEntry struct {
	Command interface{}
	Term    int
}

type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds int

	server *Server

	commitChan         chan<- CommitEntry // 这是来自客户端的日志通知通道，用于通知客户端哪些日志被提交了
	newCommitReadyChan chan struct{}      // 这是通知日志通道可以发送的信号

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex        int // 确认已提交的日志条目最大索引
	lastApplied        int // 应用到状态机的日志条目最大索引，和 commitIndex 会有延迟才要设置两个以快速响应，这个应用是在 Raft 的客户端应用（实际上可能是个数据库啥的）
	state              CMState
	electionResetEvent time.Time

	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16) // 各个 raft 节点都会有，因为各个 raft 都对应一个数据库
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	go func() {
		<-ready      // 这是用于通知本机开始运转的信号
		cm.mu.Lock() // 这里线程中需要用到 cm 的字段就都要加锁
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
	return cm
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.dlog("%v 收到新指令呈递：%v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.dlog("... log=%v", cm.log)
		return true
	}

	return false
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.state = Dead
	cm.dlog("becomes Dead")
	close(cm.newCommitReadyChan)
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args)
	}
}

type RequestVoteArgs struct {
	Term         int
	CanditateId  int
	LastLogIndex int // 候选人的最新日志条目的对应索引
	LastLogTerm  int // 候选人的最新日志条目的对应任期，这俩用来保证选举的安全性
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm() // 接收 RequestVote 机器的最新日志条目对应索引和任期
	cm.dlog("收到RequestVote请求: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... 当前任期早于RequestVote任期，变为Follower") // 来自较小分区后重连的高任期 RequestVote 会使所有结点进入 Follower 并进行选举， 但是那些个重连的节点因为日志数少不可能写入日志也不可能被选为 Leader，因为安全性检查
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CanditateId) && // 这里是为什么来着？为什么会已经投票给当前请求同伴？
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) { // 进行投票安全性检查
		reply.VoteGranted = true
		cm.votedFor = args.CanditateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote 应答：%+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int        // 紧接在新条目之前的条目索引
	PrevLogTerm  int        // 条目索引对应的任期，这俩用来判断追随者的日志在哪一处（最晚）和领导者的匹配，然后将该处日志后面的所有领导者的日志都复制到追随者节点上
	Entries      []LogEntry // 需要报错的日志条目？
	LeaderCommit int        // 领导者的 commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	cm.dlog("收到AppendEntries请求：%+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... 当前任期早于AppendEntries任期，变为Follower")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.now() // 收到心跳更新自己的选举时间

		if args.PrevLogIndex == -1 || // 对于现有 Leader 来说，PrevLogIndex 是本机在那里记录过的已经记录过的最后一个索引
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) { // 只要当前日志的索引和任期都能对应上，那么前面的日志都是对应的，否则返回 false 并且把索引提前一位来看是否对应
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1 // 找到插入点，因为可能现有领导者并不知道本机上一次匹配到哪里了
			newEntriesIndex := 0                    // 需要找到哪边开始插入，不过应该也可以直接把 Entries 内的所有内容都 append 上去吧其实

			for { // 这一段实际上有点多余？但是论文里是这么写的...
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) { // 如果是空的心跳就 ignore 了
				cm.dlog("... 从索引 index %d 处开始插入日志 %v ", logInsertIndex, args.Entries[newEntriesIndex:])
				cm.log = append(cm.log[logInsertIndex:], args.Entries[newEntriesIndex:]...)
				cm.dlog("... 现在日志内容为: %v", cm.log)
			}

			if args.LeaderCommit > cm.commitIndex { // AppendEntries 不但传递了指令体，还传递了 commit 来看是否有要提交 raft 对应数据库的日志
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... 设置 commitIndex 为 %d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{} // 通知本 raft 节点的客户端（数据库）爷又有哪些日志被确认提交了
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries 应答: %+v", *reply)
	return nil
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	cm.dlog("选举定时器启动 (%v), 任期term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond) // 计时器轮询
	defer ticker.Stop()                             // 记得要停止 ticker
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Canditate && cm.state != Follower { // 为 Canditate 时不退出是因为选举还没有出结果，万一失败了新任期的定时器不能消失，旧任期的定时器通过 term 可以判断消除而不是通过状态
			cm.dlog("选举定时器中的服务器状态 state=%s, 退出", cm.state)
			cm.mu.Unlock()
		}

		if termStarted != cm.currentTerm {
			cm.dlog("选举定时器中的term从 %d 变为 %d, 退出", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionResetEvent); elpased >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Canditate
	cm.currentTerm += 1
	savedCurrentTerm = cm.currentTerm
	cm.electionResetEvent = time.now()
	cm.votedFor = cm.id
	cm.dlog("变为候选人 (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived int32 = 1

	for _, peerId := range cm.peerIds {
		go func(peerId int) { // RPC 请求非阻塞
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm() // 获取当前记录的日志索引和任期，用于选举安全性检查
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CanditateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("向服务器 %d 发送RequestVote请求: %+v", peerId, args)
			var reply RequestVoteReply
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				cm.dlog("收到RequestVote应答 : %+v", reply)

				if cm.state != Canditate { // 在接收到之前状态已经改变了，比如成为 Leader 或者 Follower
					cm.dlog("等待RequestVote回复时，状态变为 %v", cm.state)
					return
				}

				if reply.Term > savedCurrentTerm { // 这个 savedCurrent 就算是当上 Leader 也是这个任期值，如果有比它大的，那必然这次的选举没有什么用呗
					cm.dlog("当前任期早于RequestVote应答中的任期")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm { // 这个判断有必要吗？
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(cm.peerIds)+1 {
							cm.dlog("以 %d 票数胜选，成为Leader", votes)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("变为Follower, 任期term=%d; 日志log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer() // 因为本机的任期变了，任期一变就需要开启新任期的 runElectionTimer，然后旧任期的会自动消失
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	for _, peerId := range cm.peerIds { // 成为 Leader 后，需要将自身记录的同伴节点的已匹配索引数组初始化，在 AppendEntries 内进行变化
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("成为Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader { // 在心跳中发现自己的任期低了
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm // 涉及到的变量都需要记录下来，如果里面使用异步的方法，本身是会改变的
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId] // nextIndex 初始化时保存的是长度，就是已有日志索引+1
			prevLogIndex := ni - 1     // nextIndex 的前一个，判断 peerId 的日志是否已经到 prevLogIndex 了
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commmitIndex,
			}
			cm.mu.Unlock()

			cm.dlog("发送 AppendEntries 请求到服务器 %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					cm.dlog("当前任期term早于heartbeat应答中的任期") // 旧任期的 Leader 在变为 Follower 后，在下一次新的心跳中回滚自己宕机后来自客户端的指令记录
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success { // 将 entries 成功添加到追随者的日志记录中了
						cm.nextIndex[peerId] = ni + len(entries) // 就是设置值那个时候的日志长度
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						cm.dlog("从服务器 %d 接收到 AppendEntries 请求的成功应答: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)

						savedCommitIndex := cm.commitIndex // 从本 Leader 的 commitIndex 开始，看最晚到哪个日志被大多数节点记录了
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i // cm.commitIndex 变化就是说承认这个之前的日志是要发送到状态机的，但是这里还只是确认，下面才是真的发送
								}
							}
						}

						if cm.commitIndex != savedCommitIndex { // Leader commit 完之后，下一次心跳才会将 comit 值发送给 Follower
							cm.dlog("Leader 设置 commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{} // 通知本 raft 节点的客户端（数据库）爷有新的日志需要应用到状态机了
						}
					} else {
						cm.nextIndex[peerId] = ni - 1 // 向前移动一位索引，因为这个索引不匹配啊，需要有更多的日志在下次心跳发送到跟追随者点上
						cm.dlog("从服务器 %d 接收到 AppendEntries 请求的失败应答: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func (cm *ConsensusModule) commitChanSender() { // 保持运行，监听有哪些条目要发送给客户端应用到状态机上
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied // 已应用到状态机上的日志索引
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied { // Applied 值只在本机和 commit 交流，而 commit 在 raft 节点之间进行传递
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex // 假装已经应用上去了，下面反正也是阻塞的
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender 向客户端发送指令 entries=%v, 最近执行指令索引savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries { // 应用到状态机上的 command 的任期是应用时 Leader 的任期，而不是 commit 到节点日志数组时时 Leader 的任期
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}

	cm.dlog("commitChanSender 向客户端发送指令完成")
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
