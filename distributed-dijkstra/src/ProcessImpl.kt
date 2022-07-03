package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val environment: Environment) : Process {
    private var distance: Long = -1
    private var parentId: Int = -1
    private var balance = 0
    private var childrenCount = 0

    override fun onMessage(srcId: Int, message: Message) {
        when (message) {
            is Green -> {
                --childrenCount
            }
            is Ok -> {
                --balance
                if (message.isChild) ++childrenCount
            }
            is UpdateDistance -> {
                if (parentId == -1) {
                    parentId = srcId
                    environment.send(srcId, Ok(true))
                } else {
                    environment.send(srcId, Ok(false))
                }

                if (minDist(message.distance)) {
                    for (edge in environment.neighbours) {
                        environment.send(edge.key, UpdateDistance(distance + edge.value))
                        ++balance
                    }
                }
            }
        }

        checkGreen()
    }

    override fun getDistance(): Long? {
        return if (distance == -1L) null else distance
    }

    override fun startComputation() {
        distance = 0
        parentId = environment.pid

        for (edge in environment.neighbours) {
            environment.send(edge.key, UpdateDistance(distance + edge.value))
            ++balance
        }
        checkGreen()
    }

    private fun checkGreen() {
        if (balance == 0 && childrenCount == 0) {
            if (parentId == environment.pid) {
                environment.finishExecution()
            }
            else {
                environment.send(parentId, Green)
            }
            parentId = -1
        }
    }


    private fun minDist(newDistance: Long): Boolean {
        if (distance == -1L || distance > newDistance) {
            distance = newDistance
            return true
        }

        return false
    }
}