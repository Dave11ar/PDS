package dijkstra.messages

sealed class Message

object Green : Message()

data class Ok(val isChild: Boolean) : Message()

data class UpdateDistance(val distance: Long) : Message()