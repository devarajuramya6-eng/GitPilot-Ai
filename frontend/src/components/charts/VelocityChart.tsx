/**
 * VelocityChart Component
 * Issue velocity area chart with time range selector, multiple series, annotations, and trend line
 *
 * Part of GitPilot AI — Intelligent GitHub Issue Triage Platform
 *
 * @module VelocityChart
 * @version 1.0.0
 */

import React, {
  useState, useEffect, useCallback, useMemo, useRef,
  useReducer, useContext, createContext, forwardRef, memo,
  Suspense, lazy, startTransition
} from 'react';


// ── Type Definitions ──────────────────────────────────────────────────────────

/** Priority levels for issues and tasks */
export type Priority = 'critical' | 'high' | 'medium' | 'low';

/** Issue status values */
export type IssueStatus = 'open' | 'in_progress' | 'resolved' | 'closed' | 'wont_fix';

/** Issue type classification */
export type IssueType = 'bug' | 'feature' | 'enhancement' | 'documentation' | 'question' | 'security';

/** Developer skill proficiency level */
export type SkillLevel = 'novice' | 'intermediate' | 'advanced' | 'expert';

/** Sort direction */
export type SortDirection = 'asc' | 'desc';

/** Filter operator */
export type FilterOperator = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'in' | 'between';

/** Time range */
export type TimeRange = '1h' | '6h' | '24h' | '7d' | '30d' | '90d' | '1y' | 'custom';

/** Theme */
export type Theme = 'light' | 'dark' | 'system';

/** API response wrapper */
export interface ApiResponse<T> {
  data: T;
  error: string | null;
  status: number;
  timestamp: string;
  requestId: string;
  pagination?: PaginationMeta;
}

/** Pagination metadata */
export interface PaginationMeta {
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
  hasNext: boolean;
  hasPrev: boolean;
}

/** Filter configuration */
export interface FilterConfig {
  field: string;
  operator: FilterOperator;
  value: unknown;
  label?: string;
}

/** Sort configuration */
export interface SortConfig {
  field: string;
  direction: SortDirection;
}

/** GitHub Issue representation */
export interface Issue {
  id: string;
  number: number;
  title: string;
  body: string;
  state: IssueStatus;
  priority: Priority;
  type: IssueType;
  component: string;
  assignee: Developer | null;
  labels: string[];
  milestone: string | null;
  repositoryId: string;
  createdAt: string;
  updatedAt: string;
  closedAt: string | null;
  qualityScore: number;
  confidenceScore: number;
  duplicateOf: string | null;
  commentCount: number;
  url: string;
  author: string;
  mlPredictions?: IssuePredictions;
}

/** ML predictions for an issue */
export interface IssuePredictions {
  type: { label: IssueType; confidence: number; }[];
  priority: { label: Priority; confidence: number; }[];
  component: { label: string; confidence: number; }[];
  duplicates: { id: string; title: string; similarity: number; }[];
  suggestedAssignees: { developer: Developer; score: number; reason: string; }[];
  qualityScore: number;
  complexityScore: number;
  estimatedEffort: number;
}

/** Developer profile */
export interface Developer {
  id: string;
  name: string;
  email: string;
  avatarUrl: string;
  githubLogin: string;
  role: string;
  skills: Skill[];
  workload: Workload;
  performance: PerformanceMetrics;
  availability: Availability;
  components: string[];
  joinedAt: string;
  isActive: boolean;
}

/** Developer skill */
export interface Skill {
  name: string;
  level: SkillLevel;
  score: number;
  verifiedAt: string;
  source: 'self' | 'inferred' | 'peer' | 'assessment';
}

/** Developer workload */
export interface Workload {
  openIssues: number;
  capacity: number;
  utilizationPercent: number;
  trend: 'increasing' | 'stable' | 'decreasing';
  estimatedHoursRemaining: number;
}

/** Performance metrics */
export interface PerformanceMetrics {
  resolutionRate: number;
  avgResolutionTimeDays: number;
  qualityScore: number;
  collaborationScore: number;
  velocityScore: number;
  period: string;
}

/** Developer availability */
export interface Availability {
  isAvailable: boolean;
  nextAvailableAt: string | null;
  timezone: string;
  workingHours: { start: string; end: string; };
  vacationDates: string[];
}

/** Repository info */
export interface Repository {
  id: string;
  name: string;
  fullName: string;
  description: string;
  url: string;
  language: string;
  stars: number;
  forks: number;
  openIssueCount: number;
  isConnected: boolean;
  lastSyncAt: string;
  webhookStatus: 'active' | 'inactive' | 'error';
  healthScore: number;
}

/** Metrics data point */
export interface MetricsDataPoint {
  timestamp: string;
  value: number;
  label?: string;
}

/** Chart series */
export interface ChartSeries {
  id: string;
  name: string;
  data: MetricsDataPoint[];
  color?: string;
  visible?: boolean;
}

// ── Component Props Interface ─────────────────────────────────────────────────

export interface VelocityChartProps {
    items?: any[]; // List of items to display
  onSelect?: (id: string) => void; // Selection callback
  onAction?: (action: string, id: string) => void; // Action callback
  loading?: boolean; // Loading state
  error?: string | null; // Error message
  filters?: FilterConfig[]; // Active filters
  sortConfig?: SortConfig; // Sort configuration
  pagination?: PaginationMeta; // Pagination state
  theme?: Theme; // Color theme
  onFilterChange?: (filters: FilterConfig[]) => void; // Filter change
  onSortChange?: (sort: SortConfig) => void; // Sort change
  onPageChange?: (page: number) => void; // Page change
  onRefresh?: () => void; // Refresh callback
  onExport?: (format: string) => void; // Export callback
  series?: ChartSeries[]; // 
  timeRange?: TimeRange; // 
  showTrendLine?: boolean; // 
  height?: number; // 
  annotations?: any[]; // 
  className?: string;
  style?: React.CSSProperties;
  'data-testid'?: string;
  'aria-label'?: string;
}

// ── Context ───────────────────────────────────────────────────────────────────

interface VelocityChartContextValue {
  theme: Theme;
  isLoading: Record<string, boolean>;
  errors: Record<string, string | null>;
  refetch: (key: string) => void;
}

const VelocityChartContext = createContext<VelocityChartContextValue | null>(null);

const useVelocityChartContext = () => {
  const ctx = useContext(VelocityChartContext);
  if (!ctx) throw new Error('useVelocityChartContext must be within VelocityChartProvider');
  return ctx;
};

// ── Action Types (Reducer) ────────────────────────────────────────────────────

type VelocityChartAction =
  | { type: 'SET_LOADING'; key: string; value: boolean }
  | { type: 'SET_ERROR'; key: string; error: string | null }
  | { type: 'RESET' }
  | { type: 'BATCH_UPDATE'; updates: Partial<VelocityChartState> };

interface VelocityChartState {
  loading: Record<string, boolean>;
  errors: Record<string, string | null>;
  data: Record<string, unknown>;
  lastUpdated: Record<string, string>;
}

function velocityChartReducer(state: VelocityChartState, action: VelocityChartAction): VelocityChartState {
  switch (action.type) {
    case 'SET_LOADING':
      return { ...state, loading: { ...state.loading, [action.key]: action.value } };
    case 'SET_ERROR':
      return { ...state, errors: { ...state.errors, [action.key]: action.error } };
    case 'RESET':
      return { loading: {}, errors: {}, data: {}, lastUpdated: {} };
    case 'BATCH_UPDATE':
      return { ...state, ...action.updates };
    default:
      return state;
  }
}


// ── Sub-component: ChartLegend ────────────────────────────────────────────
interface ChartLegendProps {
    series: ChartSeries[];
  onToggle: (id: string) => void;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const ChartLegend: React.FC<ChartLegendProps> = ({
  series, onToggle
  , className = '', style, testId
}) => {
  const [localState, setLocalState] = React.useState<Record<string, any>>({});
  const [isVisible, setIsVisible] = React.useState(true);

  const handleLocalAction = React.useCallback((action: string) => {
    setLocalState(prev => ({ ...prev, [action]: Date.now() }));
  }, []);

  if (!isVisible) return null;

  return (
    <div
      className={`chartlegend-container ${className}`}
      style={style}
      data-testid={testId || 'chartlegend'}
      role="region"
      aria-label="ChartLegend"
    >
      {/* Chart legend with toggle */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">ChartLegend</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: TimeRangeSelector ────────────────────────────────────────────
interface TimeRangeSelectorProps {
    value: TimeRange;
  onChange: (r: TimeRange) => void;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const TimeRangeSelector: React.FC<TimeRangeSelectorProps> = ({
  value, onChange
  , className = '', style, testId
}) => {
  const [localState, setLocalState] = React.useState<Record<string, any>>({});
  const [isVisible, setIsVisible] = React.useState(true);

  const handleLocalAction = React.useCallback((action: string) => {
    setLocalState(prev => ({ ...prev, [action]: Date.now() }));
  }, []);

  if (!isVisible) return null;

  return (
    <div
      className={`timerangeselector-container ${className}`}
      style={style}
      data-testid={testId || 'timerangeselector'}
      role="region"
      aria-label="TimeRangeSelector"
    >
      {/* Time range selector */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">TimeRangeSelector</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: ChartTooltip ────────────────────────────────────────────
interface ChartTooltipProps {
    x: number;
  y: number;
  data: any;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const ChartTooltip: React.FC<ChartTooltipProps> = ({
  x, y, data
  , className = '', style, testId
}) => {
  const [localState, setLocalState] = React.useState<Record<string, any>>({});
  const [isVisible, setIsVisible] = React.useState(true);

  const handleLocalAction = React.useCallback((action: string) => {
    setLocalState(prev => ({ ...prev, [action]: Date.now() }));
  }, []);

  if (!isVisible) return null;

  return (
    <div
      className={`charttooltip-container ${className}`}
      style={style}
      data-testid={testId || 'charttooltip'}
      role="region"
      aria-label="ChartTooltip"
    >
      {/* SVG chart tooltip */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">ChartTooltip</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: AxisLabel ────────────────────────────────────────────
interface AxisLabelProps {
    value: string;
  x: number;
  y: number;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const AxisLabel: React.FC<AxisLabelProps> = ({
  value, x, y
  , className = '', style, testId
}) => {
  const [localState, setLocalState] = React.useState<Record<string, any>>({});
  const [isVisible, setIsVisible] = React.useState(true);

  const handleLocalAction = React.useCallback((action: string) => {
    setLocalState(prev => ({ ...prev, [action]: Date.now() }));
  }, []);

  if (!isVisible) return null;

  return (
    <div
      className={`axislabel-container ${className}`}
      style={style}
      data-testid={testId || 'axislabel'}
      role="region"
      aria-label="AxisLabel"
    >
      {/* Chart axis label */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">AxisLabel</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: AnnotationMarker ────────────────────────────────────────────
interface AnnotationMarkerProps {
    x: number;
  label: string;
  color: string;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const AnnotationMarker: React.FC<AnnotationMarkerProps> = ({
  x, label, color
  , className = '', style, testId
}) => {
  const [localState, setLocalState] = React.useState<Record<string, any>>({});
  const [isVisible, setIsVisible] = React.useState(true);

  const handleLocalAction = React.useCallback((action: string) => {
    setLocalState(prev => ({ ...prev, [action]: Date.now() }));
  }, []);

  if (!isVisible) return null;

  return (
    <div
      className={`annotationmarker-container ${className}`}
      style={style}
      data-testid={testId || 'annotationmarker'}
      role="region"
      aria-label="AnnotationMarker"
    >
      {/* Vertical annotation line */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">AnnotationMarker</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};


// ── Hooks ─────────────────────────────────────────────────────────────────────

function useVelocityChartLogic(props: VelocityChartProps) {
  const [state, dispatch] = useReducer(velocityChartReducer, {
    loading: {},
    errors: {},
    data: {},
    lastUpdated: {},
  });

  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<Record<string, boolean>>({});
    const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [activeTab, setActiveTab] = useState<string>('overview');

  const mountedRef = useRef(true);
  const abortControllerRef = useRef<AbortController | null>(null);

  const svgRef = useRef<SVGSVGElement>(null);
  const [visibleSeries, setVisibleSeries] = useState<Set<string>>(new Set());
  const [tooltip, setTooltip] = useState<{ x: number; y: number; data: any } | null>(null);
  const [chartWidth, setChartWidth] = useState(800);
  const [chartHeight] = useState(props.height ?? 300);
  // Compute chart dimensions
  const margin = useMemo(() => ({ top: 20, right: 30, bottom: 40, left: 60 }), []);
  const innerWidth = chartWidth - margin.left - margin.right;
  const innerHeight = chartHeight - margin.top - margin.bottom;
  // Compute data ranges
  const allValues = useMemo(() => (props.series ?? []).flatMap(s => s.data.map(d => d.value)), [props.series]);
  const maxValue = useMemo(() => Math.max(...allValues, 1), [allValues]);
  const minValue = useMemo(() => Math.min(...allValues, 0), [allValues]);
  // Scale functions
  const xScale = useCallback((index: number, total: number) => margin.left + (index / (total - 1 || 1)) * innerWidth, [innerWidth, margin]);
  const yScale = useCallback((value: number) => margin.top + innerHeight - ((value - minValue) / (maxValue - minValue || 1)) * innerHeight, [innerHeight, margin, maxValue, minValue]);
  // Generate SVG path for area chart
  const generateAreaPath = useCallback((data: MetricsDataPoint[]) => {
    if (!data.length) return '';
    const points = data.map((d, i) => `${xScale(i, data.length)},${yScale(d.value)}`).join(' L ');
    const firstX = xScale(0, data.length), lastX = xScale(data.length - 1, data.length);
    const baseY = yScale(0);
    return `M ${firstX},${baseY} L ${points} L ${lastX},${baseY} Z`;
  }, [xScale, yScale]);
  // Generate SVG path for line chart
  const generateLinePath = useCallback((data: MetricsDataPoint[]) => {
    return data.map((d, i) => `${i === 0 ? 'M' : 'L'} ${xScale(i, data.length)},${yScale(d.value)}`).join(' ');
  }, [xScale, yScale]);
  // Compute Y-axis ticks
  const yTicks = useMemo(() => Array.from({ length: 6 }, (_, i) => minValue + (i / 5) * (maxValue - minValue)), [minValue, maxValue]);
  // Compute X-axis labels
  const xLabels = useMemo(() => {
    const data = props.series?.[0]?.data ?? [];
    const step = Math.ceil(data.length / 6);
    return data.filter((_, i) => i % step === 0);
  }, [props.series]);
  // Resize observer
  useEffect(() => {
    const observer = new ResizeObserver(entries => {
      for (const entry of entries) setChartWidth(entry.contentRect.width);
    });
    if (svgRef.current) observer.observe(svgRef.current.parentElement!);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    abortControllerRef.current = new AbortController();
    return () => {
      mountedRef.current = false;
      abortControllerRef.current?.abort();
    };
  }, []);

  const handleSelect = (id: string, event?: React.MouseEvent) => {
    try {
      // Handle item selection
      console.log('handleSelect triggered');
      setLoading(prev => ({ ...prev, handleSelect: true }));
      setSelectedIds(prev => { const next = new Set(prev); next.has(id) ? next.delete(id) : next.add(id); return next; });
    } catch (error) {
      console.error('handleSelect error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleSelect: false }));
    }
  };
  const handleSelectAll = () => {
    try {
      // Select or deselect all items
      console.log('handleSelectAll triggered');
      setLoading(prev => ({ ...prev, handleSelectAll: true }));
      await new Promise(r => setTimeout(r, 100));
    } catch (error) {
      console.error('handleSelectAll error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleSelectAll: false }));
    }
  };
  const handleSearch = (query: string) => {
    try {
      // Handle search input
      console.log('handleSearch triggered');
      setLoading(prev => ({ ...prev, handleSearch: true }));
      setSearchQuery(query);
    } catch (error) {
      console.error('handleSearch error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleSearch: false }));
    }
  };
  const handleFilter = (filter: FilterConfig) => {
    try {
      // Apply a filter
      console.log('handleFilter triggered');
      setLoading(prev => ({ ...prev, handleFilter: true }));
      await new Promise(r => setTimeout(r, 100));
    } catch (error) {
      console.error('handleFilter error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleFilter: false }));
    }
  };
  const handleSort = (field: string) => {
    try {
      // Toggle sort on field
      console.log('handleSort triggered');
      setLoading(prev => ({ ...prev, handleSort: true }));
      await new Promise(r => setTimeout(r, 100));
    } catch (error) {
      console.error('handleSort error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleSort: false }));
    }
  };
  const handleExpand = (id: string) => {
    try {
      // Toggle expand item
      console.log('handleExpand triggered');
      setLoading(prev => ({ ...prev, handleExpand: true }));
      setExpandedIds(prev => { const next = new Set(prev); next.has(id) ? next.delete(id) : next.add(id); return next; });
    } catch (error) {
      console.error('handleExpand error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleExpand: false }));
    }
  };
  const handleContextMenu = (event: React.MouseEvent, id: string) => {
    try {
      // Show context menu
      console.log('handleContextMenu triggered');
      setLoading(prev => ({ ...prev, handleContextMenu: true }));
      event.preventDefault(); setContextMenu({ x: event.clientX, y: event.clientY, id });
    } catch (error) {
      console.error('handleContextMenu error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleContextMenu: false }));
    }
  };
  const handleBulkAction = async (action: string) => {
    try {
      // Handle bulk action on selected items
      console.log('handleBulkAction triggered');
      setLoading(prev => ({ ...prev, handleBulkAction: true }));
      await new Promise(r => setTimeout(r, 100));
    } catch (error) {
      console.error('handleBulkAction error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleBulkAction: false }));
    }
  };
  const handleExport = async (format: 'csv' | 'json' | 'xlsx') => {
    try {
      // Export selected or all items
      console.log('handleExport triggered');
      setLoading(prev => ({ ...prev, handleExport: true }));
      await new Promise(r => setTimeout(r, 100));
    } catch (error) {
      console.error('handleExport error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleExport: false }));
    }
  };
  const handleRefresh = async () => {
    try {
      // Refresh data
      console.log('handleRefresh triggered');
      setLoading(prev => ({ ...prev, handleRefresh: true }));
      await new Promise(r => setTimeout(r, 100));
    } catch (error) {
      console.error('handleRefresh error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleRefresh: false }));
    }
  };
  const handleTabChange = (tab: string) => {
    try {
      // Switch active tab
      console.log('handleTabChange triggered');
      setLoading(prev => ({ ...prev, handleTabChange: true }));
      setActiveTab(tab);
    } catch (error) {
      console.error('handleTabChange error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleTabChange: false }));
    }
  };
  const handleViewModeChange = (mode: 'grid' | 'list' | 'kanban') => {
    try {
      // Change view mode
      console.log('handleViewModeChange triggered');
      setLoading(prev => ({ ...prev, handleViewModeChange: true }));
      setViewMode(mode);
    } catch (error) {
      console.error('handleViewModeChange error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleViewModeChange: false }));
    }
  };
  const handleTimeRangeChange = (range: TimeRange) => {
    try {
      // Update time range
      console.log('handleTimeRangeChange triggered');
      setLoading(prev => ({ ...prev, handleTimeRangeChange: true }));
      setTimeRange(range);
    } catch (error) {
      console.error('handleTimeRangeChange error:', error);
      setError(error instanceof Error ? error.message : 'Operation failed');
    } finally {
      setLoading(prev => ({ ...prev, handleTimeRangeChange: false }));
    }
  };

  return {
    state,
    dispatch,
    error,
    loading,
    selectedIds, searchQuery, activeTab,
    handleSelect, handleSelectAll, handleSearch, handleFilter, handleSort, handleExpand, handleContextMenu, handleBulkAction, handleExport, handleRefresh, handleTabChange, handleViewModeChange, handleTimeRangeChange,
  };
}

// ── Main Component ────────────────────────────────────────────────────────────

/**
 * VelocityChart
 *
 * Issue velocity area chart with time range selector, multiple series, annotations, and trend line
 *
 * @example
 * ```tsx
 * <VelocityChart
 *   items={[]}
 *   className="my-4"
 * />
 * ```
 */
export const VelocityChart: React.FC<VelocityChartProps> = memo((props) => {
  const {
    items, onSelect, onAction, loading, error, filters, sortConfig, pagination, theme, onFilterChange, onSortChange, onPageChange, onRefresh, onExport, series, timeRange, showTrendLine, height, annotations,
    className = '',
    style,
    'data-testid': testId = 'velocitychart',
    'aria-label': ariaLabel = 'Issue velocity area chart with time range selector, multiple series, annotations, and trend line',
  } = props;

  const logic = useVelocityChartLogic(props);

  const contextValue = useMemo<VelocityChartContextValue>(() => ({
    theme: 'light',
    isLoading: logic.loading,
    errors: {},
    refetch: (key: string) => console.log('refetch', key),
  }), [logic.loading]);

  if (logic.error) {
    return (
      <div
        className="rounded-lg border border-red-200 bg-red-50 p-6 text-center"
        role="alert"
        aria-live="assertive"
      >
        <div className="text-red-600 font-semibold mb-2">Error</div>
        <p className="text-red-500 text-sm">{logic.error}</p>
        <button
          className="mt-4 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors"
          onClick={() => logic.state.errors && logic.dispatch({ type: 'RESET' })}
        >
          Try Again
        </button>
      </div>
    );
  }

  return (
    <VelocityChartContext.Provider value={contextValue}>
      <div
        className={`velocitychart flex flex-col gap-4 ${className}`}
        style={style}
        data-testid={testId}
        aria-label={ariaLabel}
      >
        {/* Header */}
        <header className="flex items-center justify-between pb-4 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-bold text-gray-900 dark:text-white">
              \1elocity \1hart
            </h2>
            <p className="text-sm text-gray-500 mt-1">Issue velocity area chart with time range selector, multiple series, annotations, and trend line</p>
          </div>
          <div className="flex items-center gap-2">
            {logic.loading['main'] && (
              <div className="h-5 w-5 animate-spin rounded-full border-2 border-blue-600 border-t-transparent" />
            )}
          </div>
        </header>

        {/* Content */}
        <main className="flex-1 min-h-0">
          
        {/* ── Time Range Controls ── */}
        <section className="section p-4" aria-label="Time Range Controls">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Time Range Controls
            </h3>
            <span className="text-sm text-gray-500"></span>
          </div>
          <div className='time-controls flex gap-2'></div>
        </section>
        {/* ── SVG Chart ── */}
        <section className="section p-4" aria-label="SVG Chart">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              SVG Chart
            </h3>
            <span className="text-sm text-gray-500"></span>
          </div>
          <div className='chart-container relative w-full'></div>
        </section>
        {/* ── Legend ── */}
        <section className="section p-4" aria-label="Legend">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Legend
            </h3>
            <span className="text-sm text-gray-500"></span>
          </div>
          <div className='chart-legend flex flex-wrap gap-3 justify-center'></div>
        </section>
        </main>

        {/* Footer */}
        <footer className="pt-4 border-t border-gray-200 dark:border-gray-700 text-xs text-gray-400 flex justify-between">
          <span>VelocityChart v1.0.0</span>
          <span>GitPilot AI</span>
        </footer>
      </div>
    </VelocityChartContext.Provider>
  );
});

VelocityChart.displayName = 'VelocityChart';

export default VelocityChart;
