import java.util.*
import kotlin.Comparator
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class ConsistentHashImpl<K> : ConsistentHash<K> {
    private val nodeToShard = TreeSet(Comparator<Pair<Int, Shard>> { a, b -> a.first.compareTo(b.first) })
    private lateinit var first: Shard

    private fun contains(range: HashRange, newRange: HashRange): Boolean {
        if (range == newRange) return false

        return if (range.leftBorder > range.rightBorder) {
            range.leftBorder <= newRange.leftBorder || newRange.rightBorder <= range.rightBorder
        } else {
            range.leftBorder <= newRange.leftBorder && newRange.rightBorder <= range.rightBorder
        }
    }

    private fun fixCircle(set: MutableSet<HashRange>) {
        if (set.size <= 1) return
        var first = set.first()
        var last = set.first()
        set.forEach { range ->
            if (range.leftBorder < first.leftBorder) first = range
            if (range.rightBorder > last.rightBorder) last = range
        }

        if (first.leftBorder == Int.MIN_VALUE || last.rightBorder == Int.MAX_VALUE) {
            val circle = HashRange(last.leftBorder, first.rightBorder)
            set.remove(first)
            set.remove(last)

            set.removeIf { hash -> contains(circle, hash) }

            set.add(circle)
        }
    }

    private fun addToResultR(result: HashMap<Shard, MutableSet<HashRange>>) = { curShard: Shard, newRange: HashRange ->
        if (!result.getOrPut(curShard) { HashSet() }.any { range -> contains(range, newRange) }) {
            result[curShard]!!.add(newRange)
        }

        result[curShard]!!.removeIf { range -> contains(newRange, range) }
    }

    override fun getShardByKey(key: K): Shard {
        val elem = Pair(key.hashCode(), Shard(""))
        val ceiling = nodeToShard.ceiling(elem)
        return ceiling?.second ?: first
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val result = HashMap<Shard, MutableSet<HashRange>>()
        val addToResult = addToResultR(result)

        if (!nodeToShard.isEmpty()) {
            vnodeHashes.forEach { hash ->
                val newNode = Pair(hash, newShard)

                val floor = nodeToShard.floor(newNode)
                val ceiling = nodeToShard.ceiling(newNode)

                addToResult(
                    ceiling?.second ?: first,
                    HashRange(
                        if (floor != null) {
                            floor.first + 1
                        } else {
                            val last = nodeToShard.last()
                            if (last.first != Int.MAX_VALUE) {
                                addToResult(first, HashRange(last.first + 1, Int.MAX_VALUE))
                            }
                            Int.MIN_VALUE
                        }, hash
                    )
                )
            }

            result.forEach { (_, set) -> fixCircle(set)}
        }

        vnodeHashes.forEach { hash -> nodeToShard.add(Pair(hash, newShard)) }
        first = nodeToShard.first().second

        return result
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val vnodeHashes = HashSet<Int>()
        val result = HashMap<Shard, MutableSet<HashRange>>()
        val addToResult = addToResultR(result)

        nodeToShard.forEach { (hash, curShard) -> if (shard == curShard) vnodeHashes.add(hash) }
        nodeToShard.removeIf { (_, curShard) -> shard == curShard }

        first = nodeToShard.first().second
        vnodeHashes.forEach { hash ->
            val newNode = Pair(hash, shard)

            val floor = nodeToShard.floor(newNode)
            val ceiling = nodeToShard.ceiling(newNode)

            val curShard = ceiling?.second ?: first
            addToResult(
                curShard,
                HashRange(
                    if (floor != null) {
                        floor.first + 1
                    } else {
                        val last = nodeToShard.last()
                        if (last.first != Int.MAX_VALUE) {
                            addToResult(first, HashRange(last.first + 1, Int.MAX_VALUE))
                        }
                        Int.MIN_VALUE
                    }, hash
                )
            )
        }

        result.forEach { (_, set) -> fixCircle(set)}
        return result
    }
}