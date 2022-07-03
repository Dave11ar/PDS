import system.MergerEnvironment
import java.util.*
import kotlin.collections.HashMap

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val remainingBatches = HashMap<Int, MutableList<T>>()

    init {
        prevStepBatches?.forEach { (id, batch) -> remainingBatches.getOrPut(id) { LinkedList() }.addAll(batch) }
    }

    override fun mergeStep(): T? {
        var index: Int? = null
        var min: T? = null

        for (i in 0 until mergerEnvironment.dataHoldersCount) {
            if (!remainingBatches.containsKey(i)) remainingBatches[i] = LinkedList(mergerEnvironment.requestBatch(i))
            val curDataHolderElems = remainingBatches[i]!!

            if (curDataHolderElems.isEmpty()) continue

            if (min == null || curDataHolderElems.first() < min) {
                index = i
                min = curDataHolderElems.first()
            }
        }

        if (index != null) {
            remainingBatches[index]!!.removeFirst()
            if (remainingBatches[index]!!.isEmpty()) {
                val list = LinkedList(mergerEnvironment.requestBatch(index))
                if (list.isEmpty()) {
                    remainingBatches.remove(index)
                } else {
                    remainingBatches[index] = list
                }
            }
        }

        return min
    }

    override fun getRemainingBatches(): Map<Int, List<T>> {
        return remainingBatches.filter { (_, value) -> value.isNotEmpty() }
    }
}