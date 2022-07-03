package mutex

/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Artyom Davydov // todo: replace with your name
 */
class ProcessImpl(private val env: Environment) : Process {
    // 0 -- fork is not ours
    // 1 -- fork is our, and it is clean
    // 2 -- fork is our, and it is dirty
    private val forks = IntArray(env.nProcesses + 1)
    private val requests = ArrayDeque<Int>()
    private var want = false

    init {
        for (i in 1..env.nProcesses) {
            if (env.processId < i) forks[i] = 2
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        when (MessageParser(message).readEnum<MsgType>()) {
            MsgType.REQ -> {
                if (forks[srcId] == 1) {
                    requests.addLast(srcId)
                } else { // forks[srcId] == 2
                    forks[srcId] = 0
                    send(srcId, MsgType.OK)
                    if (want) send(srcId, MsgType.REQ)
                }
            }
            MsgType.OK -> {
                forks[srcId] = 1
                checkLocked()
            }
        }
    }

    override fun onLockRequest() {
        want = true
        for (i in 1..env.nProcesses) {
            if (i != env.processId && forks[i] == 0) send(i, MsgType.REQ)
        }

        checkLocked()
    }

    override fun onUnlockRequest() {
        want = false
        for (i in 1..env.nProcesses) {
            if (i != env.processId) forks[i] = 2
        }
        env.unlocked()
        for (request in requests) {
            forks[request] = 0
            send(request, MsgType.OK)
        }
        requests.clear()
    }

    private fun checkLocked() {
        for (i in 1..env.nProcesses) {
            if (i != env.processId && forks[i] == 0) return
        }

        for (i in 1..env.nProcesses) {
            if (i != env.processId && forks[i] == 2) forks[i] = 1
        }

        env.locked()
    }

    private fun send(dstId: Int, msgType: MsgType) {
        env.send(dstId) {
            writeEnum(msgType)
        }
    }

    private enum class MsgType { REQ, OK }
}
