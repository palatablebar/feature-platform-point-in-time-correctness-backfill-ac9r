<template>
  <AppShell active-key="backfill">
    <PageHeader
      op="04"
      title="回填一致性"
      sub="回填任务比较「全量重算」与「基准快照 + 增量合并」的逐样本特征 JSON md5；不变量 full == incremental。"
    >
      <template #seal>
        <div>{{ tasks.length }} 个回填任务</div>
        <div>{{ auth.user?.role_label }}</div>
      </template>
    </PageHeader>

    <ErrorNotice :error="error" />
    <PermitNotice
      :allowed="auth.canBackfill"
      :role-label="auth.user?.role_label ?? ''"
      target="回填任务"
      reason="发起/重跑回填需要 owner 角色；analyst 与 viewer 只能查看一致性报告。"
    />

    <div class="grid-2">
      <section class="card">
        <div class="card-title"><h2>回填任务</h2><span class="op-index">backfill_task</span></div>
        <ul class="list-reset">
          <li v-for="task in tasks" :key="task.id">
            <button
              class="select-row"
              style="width: 100%; text-align: left"
              :class="{ 'is-on': active?.id === task.id }"
              @click="select(task.id)"
            >
              <span>
                <strong class="mono" style="font-size: 0.78rem">{{ task.code }}</strong>
                <span class="small muted" style="display: block">{{ task.name }}</span>
                <span class="small muted">
                  {{ fmtTime(task.range_start) }} → {{ fmtTime(task.range_end) }} · {{ task.mode }}
                </span>
                <span v-if="versionText(task)" class="small mono" style="display: block; color: var(--ink-soft)">
                  锁定版本 {{ versionText(task) }}
                </span>
              </span>
              <StatusTag :status="task.status" :labels="BACKFILL_STATUS" />
            </button>
          </li>
        </ul>
        <EmptyState v-if="tasks.length === 0" title="还没有回填任务" hint="seed 未执行，或任务被清空。" />
      </section>

      <section class="card">
        <div class="card-title"><h2>发起 30 天回填</h2><span class="op-index">owner only</span></div>
        <div class="field">
          <label>范围起点</label>
          <input v-model="rangeStart" :disabled="!auth.canBackfill" />
        </div>
        <div class="field">
          <label>范围终点</label>
          <input v-model="rangeEnd" :disabled="!auth.canBackfill" />
        </div>
        <div class="field">
          <label>模式</label>
          <select v-model="mode" :disabled="!auth.canBackfill">
            <option value="full">full（全量重算，同时比对增量）</option>
            <option value="incremental">incremental（增量重算，同时比对全量）</option>
          </select>
        </div>
        <div class="actions">
          <button class="btn btn-annot" :disabled="!auth.canBackfill || creating" @click="create">
            {{ creating ? '提交中…' : '发起回填' }}
          </button>
          <button class="btn btn-ghost" :disabled="!auth.canBackfill || running || !active" @click="runActive">
            {{ running ? '运行中…' : '运行选中任务' }}
          </button>
        </div>
        <p class="small muted" style="margin-top: 10px">
          默认范围来自 /meta（基于最新事件回溯 30 天）：{{ defaultRange.start ?? '—' }} → {{ defaultRange.end ?? '—' }}
        </p>
      </section>
    </div>

    <ChecksumPanel :report="active?.report ?? null" />

    <section class="card" v-if="active?.status === 'skipped'">
      <div class="card-title"><h2>重复运行已跳过</h2><span class="op-index">idempotent</span></div>
      <p class="small muted">
        该任务已有回填结果台账：重复运行不新增结果行，也不覆盖样本当前值。下方「样本行回填核对」
        保留首次运行的全部判定，可继续按状态筛选核对。
      </p>
    </section>

    <BackfillResultPanel
      :rows="store.resultRows"
      :total="store.resultTotal"
      :summary="active?.apply_summary ?? active?.report?.apply_summary ?? null"
      :current-status="store.resultFilter.status"
      :conflict-only="store.resultFilter.conflictOnly"
      @filter="onStatusFilter"
      @toggle-conflict="onToggleConflict"
    />

    <section class="card" v-if="active?.report">
      <div class="card-title"><h2>差异明细</h2><span class="op-index">first diff</span></div>
      <p v-if="(active.report.differences ?? []).length === 0" class="muted small">
        逐样本一致：diff_sample_count = 0，first_diff = null。
      </p>
      <table v-else>
        <thead>
          <tr><th>#</th><th>样本</th><th>原因</th><th>窗口</th></tr>
        </thead>
        <tbody>
          <tr v-for="(diff, index) in active.report.differences" :key="index">
            <td>{{ diff.index }}</td>
            <td class="mono">{{ diff.label_row_id }}</td>
            <td>{{ diff.reason }}</td>
            <td>{{ (diff.differing_windows ?? []).join(' / ') }}</td>
          </tr>
        </tbody>
      </table>
      <div class="tear" />
      <p class="small muted">
        负例对照：把窗口右端人为放宽 60s 后，比对器报出
        <strong class="mono">{{ active.report.self_check.diff_sample_count }}</strong>
        个差异样本并归因到
        <strong class="mono">{{ Object.keys(active.report.self_check.reason_classes).join('/') || '—' }}</strong>
        —— 说明「相等」不是比对器空转。
      </p>
    </section>

    <template #margin>
      <div class="margin-note">
        <span class="stamp">批注 · 005</span>
        <p>
          增量重算 = 基准水位前的快照 + 水位后新增事件（含迟到事件）按窗口合并；
          合并口径与全量必须逐样本 md5 相同，否则标记 inconsistent。
        </p>
      </div>
      <div class="margin-note" style="color: var(--ink-soft); border-left-color: var(--ink-line-strong)">
        <span class="stamp" style="color: var(--ink-soft); border-color: var(--ink-line-strong)">生效</span>
        <p>
          候选特征版本 effective_from ≤ 样本标签时间才允许写入；旧回填迟到时保留样本当前值并记
          skipped_stale，当前值来自更新版本时再标冲突。只替换被点名的特征列，其他特征值不重算。
        </p>
      </div>
      <div class="margin-note" style="color: var(--ink-soft); border-left-color: var(--ink-line-strong)">
        <span class="stamp" style="color: var(--ink-soft); border-color: var(--ink-line-strong)">幂等</span>
        <p>同一任务重复运行不新增 backfill_result 行、不覆盖样本值，任务置 skipped。</p>
      </div>
    </template>
  </AppShell>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppShell from '../components/common/AppShell.vue'
import EmptyState from '../components/common/EmptyState.vue'
import ErrorNotice from '../components/common/ErrorNotice.vue'
import PageHeader from '../components/common/PageHeader.vue'
import PermitNotice from '../components/common/PermitNotice.vue'
import StatusTag from '../components/common/StatusTag.vue'
import ChecksumPanel from '../components/backfill/ChecksumPanel.vue'
import BackfillResultPanel from '../components/backfill/BackfillResultPanel.vue'
import { BACKFILL_STATUS } from '../constants/enums'
import { fetchMeta } from '../api/backfill'
import { useApi } from '../hooks/useApi'
import { useAuthStore } from '../stores/auth'
import { useBackfillStore } from '../stores/backfill'
import { fmtTime } from '../utils/format'

const auth = useAuthStore()
const store = useBackfillStore()
const { error, run } = useApi()

const mode = ref('full')
const rangeStart = ref('')
const rangeEnd = ref('')
const defaultRange = ref<{ start: string; end: string }>({ start: '', end: '' })
const creating = ref(false)
const running = ref(false)

const tasks = computed(() => store.tasks)
const active = computed(() => store.active)

function versionText(task: { feature_versions?: Record<string, number> }): string {
  const entries = Object.entries(task.feature_versions ?? {})
  if (!entries.length) return ''
  return entries.map(([featureId, version]) => `#${featureId}=v${version}`).join(' · ')
}

function onStatusFilter(status: string | null) {
  run(() => store.setResultFilter({ status }))
}

function onToggleConflict() {
  run(() => store.setResultFilter({ conflictOnly: !store.resultFilter.conflictOnly }))
}

onMounted(async () => {
  await run(() => store.load())
  const meta = await run(() => fetchMeta())
  if (meta) {
    defaultRange.value = meta.default_backfill_range
    rangeStart.value = meta.default_backfill_range.start
    rangeEnd.value = meta.default_backfill_range.end
  }
  await auth.refresh()
})

async function select(id: number) {
  await run(() => store.select(id))
}

async function create() {
  creating.value = true
  const task = await run(() =>
    store.create({
      name: `30 天回填 · ${new Date().toISOString().slice(0, 10)}`,
      mode: mode.value,
      range_start: rangeStart.value,
      range_end: rangeEnd.value,
      feature_ids: [],
    }),
  )
  if (task) await run(() => store.run(task.id))
  creating.value = false
}

async function runActive() {
  if (!active.value) return
  running.value = true
  await run(() => store.run(active.value!.id))
  running.value = false
}
</script>
