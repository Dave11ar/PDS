package raft

import raft.Message.*
import kotlin.math.min

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Artyom Davydov
 */
class ProcessImpl(private val env: Environment) : Process {
    // all servers
    private var leader: Int? = null
    private var type = Type.FOLLOWER
    private var votes = 0
    private var commitIndex = 0
    private var lastTimeout = Timeout.ELECTION_TIMEOUT
    private val commandQueue = ArrayDeque<Command>()
    // leader
    private var nextIndex = Array(0) {0}
    private var matchIndex = Array(0) {0}

    init {
        startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun broadcast(getMessageById: (Int) -> Message) {
        for (destId in 1 .. env.nProcesses) {
            if (destId != env.processId) env.send(destId, getMessageById(destId))
        }
    }

    private fun startTimeout(timeout: Timeout) {
        lastTimeout = timeout
        env.startTimeout(timeout)
    }

    private fun startElection() {
        leader = null
        type = Type.CANDIDATE
        votes = 1
        env.storage.writePersistentState(PersistentState(env.storage.readPersistentState().currentTerm + 1, env.processId))
        broadcast { RequestVoteRpc(env.storage.readPersistentState().currentTerm, env.storage.readLastLogId()) }
        startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun sendHeartbeats() {
        broadcast {
            AppendEntryRpc(
                env.storage.readPersistentState().currentTerm, env.storage.readLastLogId(),
                commitIndex, null
            )
        }
        startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
    }

    private fun appendEntry(command: Command) {
        env.storage.appendLogEntry(
            LogEntry(
                LogId(env.storage.readLastLogId().index + 1, env.storage.readPersistentState().currentTerm),
                command
            )
        )
    }

    private fun sendAppend() {
        for (id in 1 .. env.nProcesses) {
            if (id != env.processId && nextIndex[id] == env.storage.readLastLogId().index) {
                env.send(
                    id, AppendEntryRpc(
                        env.storage.readPersistentState().currentTerm,
                        env.storage.readLog(env.storage.readLog(nextIndex[id])!!.id.index - 1)?.id
                            ?: START_LOG_ID,
                        commitIndex,
                        env.storage.readLog(nextIndex[id])!!
                    )
                )
            }
        }
    }

    private fun handleCommandQueue() {
        if (leader != null || type == Type.LEADER) {
            while (commandQueue.isNotEmpty()) {
                val command = commandQueue.removeLast()

                if (type == Type.LEADER) {
                    appendEntry(command)
                    sendAppend()
                } else {
                    env.send(
                        leader!!,
                        ClientCommandRpc(env.storage.readPersistentState().currentTerm, command)
                    )
                }
            }
        }
    }

    private fun updateLeader(newLeader: Int) {
        leader = newLeader
        handleCommandQueue()
    }

    private fun becomeFollower() {
        type = Type.FOLLOWER
        votes = 0
        nextIndex = Array(0) {0}
        matchIndex = Array(0) {0}
    }

    private fun becomeLeader() {
        leader = null
        type = Type.LEADER
        votes = 0
        nextIndex = Array(env.nProcesses + 1) { env.storage.readLastLogId().index + 1 }
        matchIndex = Array(env.nProcesses + 1) { 0 }
        handleCommandQueue()
        sendHeartbeats()
    }

    private fun getMaxCommitIndex(): Int {
        matchIndex[env.processId] = env.storage.readLastLogId().index
        val maxCommitIndex = matchIndex.toList().sorted()[env.nProcesses / 2 + 1]
        return if (maxCommitIndex > 0 && env.storage.readLog(maxCommitIndex)!!.id.term == env.storage.readPersistentState().currentTerm) maxCommitIndex else 0
    }

    override fun onTimeout() {
        when (lastTimeout) {
            Timeout.ELECTION_TIMEOUT -> startElection()
            Timeout.LEADER_HEARTBEAT_PERIOD -> sendHeartbeats()
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        var needElectionTimeout = false
        if (message.term > env.storage.readPersistentState().currentTerm) {
            env.storage.writePersistentState(PersistentState(message.term, null))
            becomeFollower()
            needElectionTimeout = true
        }

        when (message) {
            is AppendEntryRpc -> {
                var lastIndex: Int? = null
                if (message.term == env.storage.readPersistentState().currentTerm) {
                    becomeFollower()
                    needElectionTimeout = true
                    updateLeader(srcId)

                    var newCommitIndex: Int = message.leaderCommit
                    if (message.entry == null) {
                        lastIndex = env.storage.readLastLogId().index
                    } else {
                        val logOnIndex = env.storage.readLog(message.prevLogId.index)
                        if (message.prevLogId.index == 0 || !(logOnIndex == null || logOnIndex.id.term != message.prevLogId.term)) {
                            env.storage.appendLogEntry(message.entry)
                            lastIndex = message.entry.id.index

                            newCommitIndex = min(newCommitIndex, message.entry.id.index)
                        }
                    }

                    newCommitIndex = min(newCommitIndex, env.storage.readLastLogId().index)
                    if (commitIndex < newCommitIndex) {
                        for (index in commitIndex + 1 .. newCommitIndex) {
                            env.machine.apply(env.storage.readLog(index)!!.command)
                        }

                        commitIndex = newCommitIndex
                    }
                }

                env.send(srcId, AppendEntryResult(env.storage.readPersistentState().currentTerm, lastIndex))
            }
            is AppendEntryResult -> {
                if (type != Type.LEADER || message.term < env.storage.readPersistentState().currentTerm) return

                if (message.lastIndex == null) {
                    if (matchIndex[srcId] == 0) --nextIndex[srcId]
                } else {
                    matchIndex[srcId] = message.lastIndex
                    nextIndex[srcId] = matchIndex[srcId] + 1

                    val newCommitIndex = getMaxCommitIndex()
                    if (commitIndex < newCommitIndex) {
                        for (index in commitIndex + 1 .. newCommitIndex) {
                            val command = env.storage.readLog(index)!!.command
                            val result = env.machine.apply(command)

                            if (env.storage.readPersistentState().currentTerm == env.storage.readLog(index)!!.id.term) {
                                if (command.processId != env.processId) {
                                    env.send(
                                        command.processId,
                                        ClientCommandResult(
                                            env.storage.readPersistentState().currentTerm,
                                            result
                                        )
                                    )
                                } else {
                                    env.onClientCommandResult(result)
                                }
                            }
                        }

                        commitIndex = newCommitIndex
                    }
                }

                if (nextIndex[srcId] > 0 && nextIndex[srcId] <= env.storage.readLastLogId().index) {
                    env.send(
                        srcId, AppendEntryRpc(
                            env.storage.readPersistentState().currentTerm,
                            env.storage.readLog(nextIndex[srcId] - 1)?.id ?: START_LOG_ID,
                            commitIndex,
                            env.storage.readLog(nextIndex[srcId])!!
                        )
                    )
                }
            }
            is RequestVoteRpc -> {
                val state = env.storage.readPersistentState()
                if (type != Type.FOLLOWER) {
                    env.send(srcId, RequestVoteResult(state.currentTerm, false))
                    return
                }

                leader = null
                val voteGranted = message.term == state.currentTerm &&
                        (state.votedFor == null || state.votedFor == srcId) &&
                        env.storage.readLastLogId() <= message.lastLogId

                if (voteGranted) {
                    if (state.votedFor == null) needElectionTimeout = true
                    env.storage.writePersistentState(PersistentState(state.currentTerm, srcId))
                }
                env.send(srcId, RequestVoteResult(state.currentTerm, voteGranted))
            }
            is RequestVoteResult -> {
                if (type != Type.CANDIDATE || message.term < env.storage.readPersistentState().currentTerm) return

                if (message.voteGranted) ++votes
                if (votes > env.nProcesses / 2) {
                    becomeLeader()
                    return
                }
            }
            is ClientCommandRpc -> {
                commandQueue.addFirst(message.command)
                handleCommandQueue()
            }
            is ClientCommandResult -> {
                env.onClientCommandResult(message.result)
                updateLeader(srcId)
            }
        }

        if (needElectionTimeout) startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    override fun onClientCommand(command: Command) {
        commandQueue.addFirst(command)
        handleCommandQueue()
    }

    private enum class Type {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
}
