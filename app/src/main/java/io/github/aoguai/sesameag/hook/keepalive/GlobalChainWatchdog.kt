package io.github.aoguai.sesameag.hook.keepalive

import android.content.Context
import io.github.aoguai.sesameag.util.Log
import io.github.aoguai.sesameag.util.Log.printStackTrace

/**
 * 兜底：保证全局轮询/唤醒链不会因为一次静默失败而永久消失。
 *
 * 历史故障里轮询链的终止方式都是「records 里一条可执行的 GLOBAL_* 都不剩」：
 * - 全局计划因 `deferred_exhausted` 或超过补执行宽限被写成 FAILED/EXPIRED；
 * - upsert 注册时物理闹钟失败，撤掉新计划又没恢复旧计划；
 * - 渠道被外部清除后没有任何代码重新登记；
 * - 进程内对齐最早期限的计时器被取消（重建/超时），渠道注册已不再重新对齐。
 * 这些路径都不会有人再登记下一次轮询，用户感知就是「定时任务又睡死了」。
 */
internal object GlobalChainWatchdog {
    private const val TAG = "GlobalChainWatchdog"

    /** 多久检查一次；调度重排与主任务结束都会走到这里。 */
    private const val CHECK_INTERVAL_MS = 10 * 60 * 1000L

    @Volatile
    private var lastCheckAtMs = 0L

    /**
     * 仅在「当前会话一条可执行 GLOBAL_* 都没有」时调用 [register] 重新登记一轮。
     * 返回是否真的补登记过，便于上层留可观测证据。
     */
    fun ensureAlive(
        context: Context?,
        ownerUserId: String?,
        sessionEpoch: Long,
        register: (Context) -> Unit,
    ): Boolean {
        if (context == null) return false
        val now = System.currentTimeMillis()
        if (now - lastCheckAtMs < CHECK_INTERVAL_MS) return false
        lastCheckAtMs = now
        return try {
            val appContext = context.applicationContext ?: context
            if (PersistentScheduleRegistry.hasRunnableGlobalSchedule(ownerUserId, sessionEpoch)) {
                return false
            }
            Log.error(TAG, "当前会话没有任何可执行的全局轮询/唤醒计划，补登记一轮以免调度链彻底消失")
            register(appContext)
            true
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            printStackTrace(TAG, "全局调度链看门狗检查失败", e)
            false
        }
    }
}
