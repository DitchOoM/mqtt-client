import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.promise
import kotlin.js.Promise

actual fun <T> block(body: suspend CoroutineScope.() -> T): dynamic = runTestInternal(block = body)


fun <T> runTestInternal(
    block: suspend CoroutineScope.() -> T
): Promise<T?> {
    val promise = GlobalScope.promise {
        try {
            return@promise block()
        } catch (e: UnsupportedOperationException) {

        } catch (e: Exception) {
            e.printStackTrace()
        }
        return@promise null
    }
    promise.catch {
        if (it !is UnsupportedOperationException) {
            throw it
        }
    }
    return promise
}
