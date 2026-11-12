<template>
  <section class="card">
    <div class="card-title">
      <h2>样本行回填核对</h2>
      <span class="op-index">backfill_result · 逐样本 × 特征</span>
    </div>

    <div class="kpi-row" style="grid-template-columns: repeat(5, minmax(0, 1fr))">
      <div class="kpi">
        <div class="k">已替换 replaced</div>
        <div class="v is-ok">{{ summary?.by_status?.replaced ?? 0 }}</div>
      </div>
      <div class="kpi">
        <div class="k">已保留 retained</div>
        <div class="v">{{ summary?.by_status?.retained ?? 0 }}</div>
      </div>
      <div class="kpi">
        <div class="k">跳过·迟到 stale</div>
        <div class="v" :class="(summary?.by_status?.skipped_stale ?? 0) ? 'is-annot' : ''">
          {{ summary?.by_status?.skipped_stale ?? 0 }}
        </div>
      </div>
      <div class="kpi">
        <div class="k">跳过·重复 dup</div>
        <div class="v">{{ summary?.by_status?.skipped_duplicate ?? 0 }}</div>
      </div>
      <div class="kpi">
        <div class="k">冲突 conflict</div>
        <div class="v" :class="(summary?.conflict_count ?? 0) ? 'is-annot' : 'is-ok'">
          {{ summary?.conflict_count ?? 0 }}
        </div>
      </div>
    </div>

    <div class="actions" style="margin: var(--space-4) 0">
      <button
        v-for="opt in filterOptions"
        :key="opt.value ?? 'all'"
        class="btn btn-small"
        :class="currentStatus === opt.value ? 'btn-annot' : 'btn-ghost'"
        @click="applyStatusFilter(opt.value)"
      >
        {{ opt.label }}
      </button>
      <label class="conflict-toggle small">
        <input type="checkbox" :checked="conflictOnly" @change="toggleConflict" />
        只看冲突（旧值曾威胁新值）
      </label>
    </div>

    <p v-if="rows.length === 0" class="muted small">
      当前筛选下没有回填结果记录。任务运行后这里会逐行列出每个样本特征列的替换 / 保留 / 跳过依据。
    </p>

    <div v-else class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>样本行</th>
            <th>实体</th>
            <th>标签时间</th>
            <th>特征列</th>
            <th>候选值 / 版本</th>
            <th>当前值 / 版本</th>
            <th>版本生效时间</th>
            <th>状态</th>
            <th>判定依据</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="row.id" :class="{ 'row-conflict': row.is_conflict }">
            <td class="mono">#{{ row.sample_row_id }}</td>
            <td class="mono">{{ row.entity_key }}</td>
            <td class="mono">{{ fmtShortTime(row.label_time) }}</td>
            <td>{{ row.feature_code }}</td>
            <td class="mono">
              {{ fmtNumber(row.candidate_value) }}
              <span class="muted">/ v{{ row.candidate_version_no }}</span>
            </td>
            <td class="mono">
              {{ row.previous_value === null ? '∅' : fmtNumber(row.previous_value) }}
              <span class="muted">/ {{ row.previous_version_no === null ? '∅' : `v${row.previous_version_no}` }}</span>
            </td>
            <td class="mono">{{ fmtShortTime(row.effective_from) }}</td>
            <td>
              <span class="tag" :class="toneFor(row.status)">
                {{ BACKFILL_RESULT_STATUS[row.status] ?? row.status }}
              </span>
              <span v-if="row.is_conflict" class="tag tag-annot" style="margin-left: 4px">冲突</span>
            </td>
            <td class="small muted reason-cell">{{ row.decision_reason }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <p class="small muted" style="margin-top: var(--space-3)">
      共 {{ total }} 条结果（接口上限 200 条/页）；replaced 只改写该特征列，样本上其他已确认特征值不参与重算。
    </p>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { BACKFILL_RESULT_STATUS, BACKFILL_RESULT_TONE } from '../../constants/enums'
import type { ApplySummary, BackfillResultRow } from '../../types/backfill'
import { fmtNumber, fmtShortTime } from '../../utils/format'

const props = defineProps<{
  rows: BackfillResultRow[]
  total: number
  summary: ApplySummary | null | undefined
  currentStatus: string | null
  conflictOnly: boolean
}>()

const emit = defineEmits<{
  (e: 'filter', status: string | null): void
  (e: 'toggle-conflict'): void
}>()

const filterOptions = [
  { value: null, label: '全部' },
  { value: 'replaced', label: '已替换' },
  { value: 'retained', label: '已保留' },
  { value: 'skipped_stale', label: '旧版本迟到' },
  { value: 'skipped_duplicate', label: '重复运行' },
]

const summary = computed(() => props.summary)

function toneFor(status: string): string {
  return BACKFILL_RESULT_TONE[status] ?? 'tag-ink'
}

function applyStatusFilter(status: string | null) {
  emit('filter', status)
}

function toggleConflict() {
  emit('toggle-conflict')
}
</script>

<style scoped>
.btn-small {
  padding: 4px 10px;
  font-size: 0.72rem;
}
.conflict-toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-left: auto;
}
.table-wrap {
  overflow-x: auto;
}
.reason-cell {
  max-width: 260px;
  white-space: normal;
}
.row-conflict {
  background: var(--annot-soft);
}
</style>
