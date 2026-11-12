import { defineStore } from 'pinia'
import { ref } from 'vue'
import {
  createBackfill,
  fetchBackfillResults,
  getBackfill,
  listBackfills,
  runBackfill,
} from '../api/backfill'
import type { BackfillResultRow, BackfillTask } from '../types/backfill'

export const useBackfillStore = defineStore('backfill', () => {
  const tasks = ref<BackfillTask[]>([])
  const active = ref<BackfillTask | null>(null)
  const resultRows = ref<BackfillResultRow[]>([])
  const resultTotal = ref(0)
  const resultFilter = ref<{ status: string | null; conflictOnly: boolean }>({
    status: null,
    conflictOnly: false,
  })

  async function load() {
    const payload = await listBackfills()
    tasks.value = payload.items
    if (!active.value && tasks.value.length > 0) {
      await select(tasks.value[0].id)
    }
  }

  async function select(id: number) {
    active.value = await getBackfill(id)
    await loadResults(id)
  }

  async function loadResults(id: number = active.value?.id ?? 0) {
    const payload = await fetchBackfillResults(id, {
      status: resultFilter.value.status ?? undefined,
      conflictOnly: resultFilter.value.conflictOnly,
    })
    resultRows.value = payload.items
    resultTotal.value = payload.total
  }

  async function setResultFilter(filter: { status?: string | null; conflictOnly?: boolean }) {
    if (filter.status !== undefined) resultFilter.value.status = filter.status
    if (filter.conflictOnly !== undefined) resultFilter.value.conflictOnly = filter.conflictOnly
    if (active.value) await loadResults(active.value.id)
  }

  async function create(payload: Record<string, unknown>) {
    const task = await createBackfill(payload)
    await load()
    await select(task.id)
    return task
  }

  async function run(id: number) {
    await runBackfill(id)
    await load()
    await waitFor(id)
  }

  async function waitFor(id: number, timeoutMs = 120000): Promise<BackfillTask> {
    const started = Date.now()
    while (Date.now() - started < timeoutMs) {
      const task = await getBackfill(id)
      active.value = task
      if (['completed', 'failed', 'skipped'].includes(task.status)) {
        await load()
        return task
      }
      await new Promise((resolve) => setTimeout(resolve, 1200))
    }
    throw new Error('回填任务等待超时')
  }

  return {
    tasks,
    active,
    resultRows,
    resultTotal,
    resultFilter,
    load,
    select,
    loadResults,
    setResultFilter,
    create,
    run,
    waitFor,
  }
})
