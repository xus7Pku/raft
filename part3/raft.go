package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"
)

const DebugCM = 1

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

type LogEntry struct {
	Command interface{}
	Term    int
}

type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int

	server *Server

	storage Storage

	commitChan         chan<- CommitEntry
	newCommitReadyChan chan struct{}
	triggerAEChan      chan struct{} // 是内部通知 channel，在出现关注的更改时触发向追随者发送新的 AE 请求

	currentTerm int
	votedFor    int
	log         []LogEntry // 需要持久化的内容就是无法来自外部的信息

	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewConsensusModule(id int, peerIds []int, server *Server, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if cm.storage.HasData() {
		cm.restoreFromStorage(cm.storage)
	}

	go func() {
		<-ready
		cm.mu.Lock()
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
	cm.dlog("%v 收到新指令呈递：%v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{} // 通知新指令的呈递接收
		return true
	}

	cm.mu.Unlock()
	return false
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.state = Dead
	cm.dlog("becomes Dead")
	close(cm.newCommitReadyChan) // 通知 newCommitReadyChan 的关闭信号
}

// 从存储中加载 CM 的持久化状态，该方法在初始化时调用且避免任何的并发调用。
func (cm *ConsensusModule) restoreFromStorage(storage Storage) {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}

	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}

	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.newBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

// 将 CM 中的所有持久化状态保存到 cm.storage 中，要求 cm.mu 被锁定，每次需要持久化的状态改变就要进行一次持久化
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

type RequestVoteArgs struct {
	Term         int
	CanditateId  int
	LastLogIndex int
	LastLogTerm  int
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

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("收到RequestVote请求：%+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... 当前任期早于RequestVote任期，变为Follower")
		cm.becomeFollower(args.Term) // 转变成该任期中的 Follower
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CanditateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CanditateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlog("... RequestVote 应答：%+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int // 快速解决冲突优化，即不需要每次 AE 用于判断日志复制的量都只减 1
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
		cm.becomeFollower()
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower { // 比如说正在竞选
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... 从索引 index %d 处开始插入日志 %v", logInsertIndex, args.Entries[newEntriesIndex]:)
				cm.log = append(cm.log, args.Entries[newEntriesIndex:]...)
				cm.dlog("... 现在日志内容为: %v", cm.log)
			}

			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... 设置 commitIndex 为 %d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term // Index 一样而 Term 不对，就向前回到 Term 一样的地方的下一位去做日志获取和复制（Index 还是一样的）

				var i int
				for i = args.PrevLogIndex-1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm { // 这个冲突的任期，以及冲突的 Index 和之后的内容都要替换掉
						break
					}
				}
				reply.ConflictIndex = i + 1 // 一步到位了
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlog("AppendEntries 应答：%+v", *reply)
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
	cm.dlog("选举定时器启动 (%v)，任期 term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Canditate && cm.state != Follower {
			cm.dlog("选举定时器中的服务器状态 state=%s，退出", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("选举定时器中的term从 %d 变为 %d, 退出", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
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
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("变为候选人 (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived int32 = 1

	for _, peerId := range cm.peerIds {
		go func(peerId int) { // 需要花费较长时间的，可以开启协程
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm() // 本机的
			cm.mu.Lock()

			args := RequestVoteArgs{
				Term: 		  savedCurrentTerm,
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

				if cm.state != Canditate {
					cm.dlog("等待RequestVote回复时, 状态变为 %v", cm.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					cm.dlog("当前任期term早于RequestVote应答中的任期")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(cm.peerIds)+1 {
							// 获得票数超过一半，选举获胜，成为最新的领导者
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
	cm.electionTimeout = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	for _, peerId := range cm.peerIds { // 新的 Leader 当选，都要初始化本机记录的同伴节点的 nextIndex 和 matchIndex
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}

	cm.dlog("成为Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	go func(heartbeatTimeout time.Duration) {
		cm.leaderSendAEs() // 立刻发送 AE 给同伴节点

		t := time.NewTimer(heartbeatTimeout) // Timer 超时后需要重置才能触发
		defer t.Stop()

		for {
			doSend := false
			select { // 计时器一到，就发送；或者通道 triggerAEChan 有事需要发送（新呈递指令）
			case <-t.C:
				doSend = true
				t.Stop()
				t.Reset(heartbeatTimeout) // 重置 timer 等待下次触发
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return // 通道关闭
				}

				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			prevLogIndex := ni - 1
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
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("发送 AppendEntries 请求到服务器 %v: ni=%d, args=%+v", peerId, ni, args)

			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsunsesModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					cm.dlog("当前任期term早于heartbeat应答中的任期")
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex+1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm { // 只管自己任期的日志？
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}

								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}

						cm.dlog("从服务器 %d 接收到 AppendEntries 请求的成功应答: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, cm.nextIndex, cm.matchIndex, cm.commitIndex)

						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader 设置 commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{} // 通知客户端需要 apply
							cm.triggerAEChan <- struct{}{} // 立刻通知追随者需要 commit
						}
					} else {
						if reply.ConflictTerm >= 0 { // 此时 ConflictTerm 是冲突处的任期，ConflictIndex 是冲突处任期的最早一个索引
							lastIndexOfTerm := -1
							for i := len(cm.log)-1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm { // 和追随者冲突处任期一样的最早日志索引-1
									lastIndexOfTerm = i
									break
								}
							}

							if lastIndexOfTerm >= 0 { // 最早日志索引存在
								cm.nextIndex[peerId] = lastIndexOfTerm + 1 // 就是找到了位置（成功了）
							} else {
								cm.nextIndex[peerId] = reply.ConflictIndex // 需要进一步尝试（尝试追随者上个任期的日志对应索引）
							}
						} else {
							cm.nextIndex[peerId] = reply.ConflictIndex // 同上
						}

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

func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender 向客户端发送指令 entries=%v, 最近执行指令索引savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
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
