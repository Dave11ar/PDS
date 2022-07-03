import system.DataHolderEnvironment
import java.lang.Integer.min
import java.util.LinkedList

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private var index = 0
    private var indexCheckPoint = 0

    override fun checkpoint() {
        indexCheckPoint = index
    }

    override fun rollBack() {
        index = indexCheckPoint
    }

    override fun getBatch(): List<T> {
        if (index >= keys.size) return LinkedList()
        val result = keys.subList(index, index + min(keys.size - index, dataHolderEnvironment.batchSize))
        index += dataHolderEnvironment.batchSize
        return result
    }
}