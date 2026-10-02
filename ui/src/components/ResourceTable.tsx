import { useState } from 'react'
import { useCollection } from '@cloudscape-design/collection-hooks'
import {
  Box,
  CollectionPreferences,
  Header,
  Pagination,
  PropertyFilter,
  Table,
} from '@cloudscape-design/components'
import type {
  CollectionPreferencesProps,
  TableProps,
} from '@cloudscape-design/components'

export interface ResourceColumn<T> extends TableProps.ColumnDefinition<T> {
  /** Label shown in the filter property dropdown. */
  filterLabel?: string
  /** Text used for client-side filtering; omit to exclude the column from filters. */
  filterValue?: (item: T) => string
}

interface ResourceTableProps<T> {
  items: T[]
  columns: ResourceColumn<T>[]
  trackBy: (item: T) => string
  title: string
  description?: string
  actions?: React.ReactNode
  loading?: boolean
  onRowClick?: (item: T) => void
  emptyTitle: string
  emptyBody?: string
  selectionType?: 'single' | 'multi'
  selectedItems?: T[]
  onSelectionChange?: (items: T[]) => void
  defaultPageSize?: number
  stickyHeader?: boolean
  expandableRows?: TableProps.ExpandableRows<T>
}

/**
 * List table wired up the way the AWS Console does it: a PropertyFilter bar,
 * pagination, column/page-size preferences and optional row selection.
 */
export function ResourceTable<T>({
  items,
  columns,
  trackBy,
  title,
  description,
  actions,
  loading,
  onRowClick,
  emptyTitle,
  emptyBody,
  selectionType,
  selectedItems,
  onSelectionChange,
  defaultPageSize = 10,
  stickyHeader = true,
  expandableRows,
}: ResourceTableProps<T>) {
  const [prefs, setPrefs] = useState<CollectionPreferencesProps.Preferences>({
    pageSize: defaultPageSize,
    wrapLines: false,
    stripedRows: false,
    contentDisplay: columns.map((column) => ({ id: String(column.id), visible: true })),
  })
  const [visibleColumns, setVisibleColumns] = useState<string[]>(
    columns.map((column) => String(column.id)),
  )

  const filteringProperties = columns
    .filter((column) => column.filterValue)
    .map((column) => ({
      key: String(column.id),
      propertyLabel: column.filterLabel ?? String(column.header ?? column.id),
      groupValuesLabel: `${column.filterLabel ?? String(column.header ?? column.id)} values`,
    }))

  const {
    items: filteredItems,
    collectionProps,
    propertyFilterProps,
    paginationProps,
  } = useCollection(items, {
    propertyFiltering: {
      filteringProperties,
      empty: (
        <Box textAlign="center" color="inherit">
          <b>No resources</b>
        </Box>
      ),
      noMatch: (
        <Box textAlign="center" color="inherit">
          <b>No matches</b>
          <Box variant="p" color="inherit">
            No resources match the current filter.
          </Box>
        </Box>
      ),
    },
    pagination: { pageSize: prefs.pageSize ?? defaultPageSize },
    sorting: {},
  })

  const shownColumns = columns
    .filter((column) => visibleColumns.includes(String(column.id)))
    .map((column) => {
      // Auto-enable sorting for any filterable column by comparing the text
      // used for filtering (unless the column already defines sorting).
      if (column.filterValue && !column.sortingField && !column.sortingComparator) {
        const filterValue = column.filterValue
        return {
          ...column,
          sortingComparator: (a: T, b: T) =>
            String(filterValue(a)).localeCompare(String(filterValue(b)), undefined, {
              numeric: true,
              sensitivity: 'base',
            }),
        }
      }
      return column
    })

  return (
    <Table
      {...collectionProps}
      items={filteredItems}
      columnDefinitions={shownColumns}
      loading={loading}
      loadingText="Loading resources"
      trackBy={trackBy}
      stickyHeader={stickyHeader}
      expandableRows={expandableRows}
      selectionType={selectionType}
      selectedItems={selectedItems}
      onSelectionChange={({ detail }) => onSelectionChange?.(detail.selectedItems)}
      onRowClick={onRowClick ? ({ detail }) => onRowClick(detail.item) : undefined}
      filter={
        <PropertyFilter
          {...propertyFilterProps}
          countText={`${filteredItems.length} of ${items.length}`}
        />
      }
      pagination={<Pagination {...paginationProps} />}
      preferences={
        <CollectionPreferences
          title="Preferences"
          confirmLabel="Confirm"
          cancelLabel="Cancel"
          preferences={prefs}
          onConfirm={({ detail }) => {
            setPrefs(detail)
            const display = detail.contentDisplay ?? []
            setVisibleColumns(display.filter((item) => item.visible).map((item) => item.id))
          }}
          pageSizePreference={{
            title: 'Page size',
            options: [
              { value: 10, label: '10 resources' },
              { value: 25, label: '25 resources' },
              { value: 50, label: '50 resources' },
            ],
          }}
          wrapLinesPreference={{ label: 'Wrap lines', description: 'Allow text to wrap in cells' }}
          stripedRowsPreference={{ label: 'Striped rows', description: 'Alternate row background' }}
          contentDisplayPreference={{
            title: 'Column preferences',
            options: columns.map((column) => ({
              id: String(column.id),
              label: String(column.header ?? column.id),
            })),
          }}
        />
      }
      header={
        <Header description={description} actions={actions} counter={`(${items.length})`}>
          {title}
        </Header>
      }
      empty={
        <Box textAlign="center" color="inherit">
          <b>{emptyTitle}</b>
          {emptyBody && (
            <Box variant="p" color="inherit">
              {emptyBody}
            </Box>
          )}
        </Box>
      }
    />
  )
}
