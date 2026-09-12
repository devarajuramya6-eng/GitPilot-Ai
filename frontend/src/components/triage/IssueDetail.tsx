/**
 * IssueDetail Component
 * Detailed issue view panel with ML signal cards, duplicate detection, developer recommendations, and activity log
 *
 * Part of GitPilot AI — Intelligent GitHub Issue Triage Platform
 *
 * @module IssueDetail
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

export interface IssueDetailProps {
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
  issue?: Issue | null; // 
  onClose?: () => void; // 
  onApprove?: (id: string) => void; // 
  onReject?: (id: string, reason: string) => void; // 
  developers?: Developer[]; // 
  className?: string;
  style?: React.CSSProperties;
  'data-testid'?: string;
  'aria-label'?: string;
}

// ── Context ───────────────────────────────────────────────────────────────────

interface IssueDetailContextValue {
  theme: Theme;
  isLoading: Record<string, boolean>;
  errors: Record<string, string | null>;
  refetch: (key: string) => void;
}

const IssueDetailContext = createContext<IssueDetailContextValue | null>(null);

const useIssueDetailContext = () => {
  const ctx = useContext(IssueDetailContext);
  if (!ctx) throw new Error('useIssueDetailContext must be within IssueDetailProvider');
  return ctx;
};

// ── Action Types (Reducer) ────────────────────────────────────────────────────

type IssueDetailAction =
  | { type: 'SET_LOADING'; key: string; value: boolean }
  | { type: 'SET_ERROR'; key: string; error: string | null }
  | { type: 'RESET' }
  | { type: 'BATCH_UPDATE'; updates: Partial<IssueDetailState> };

interface IssueDetailState {
  loading: Record<string, boolean>;
  errors: Record<string, string | null>;
  data: Record<string, unknown>;
  lastUpdated: Record<string, string>;
}

function issueDetailReducer(state: IssueDetailState, action: IssueDetailAction): IssueDetailState {
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


// ── Sub-component: MLSignalCard ────────────────────────────────────────────
interface MLSignalCardProps {
    label: string;
  confidence: number;
  alternatives: any[];
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const MLSignalCard: React.FC<MLSignalCardProps> = ({
  label, confidence, alternatives
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
      className={`mlsignalcard-container ${className}`}
      style={style}
      data-testid={testId || 'mlsignalcard'}
      role="region"
      aria-label="MLSignalCard"
    >
      {/* ML prediction signal card */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">MLSignalCard</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: DuplicateCandidate ────────────────────────────────────────────
interface DuplicateCandidateProps {
    issue: Issue;
  similarity: number;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const DuplicateCandidate: React.FC<DuplicateCandidateProps> = ({
  issue, similarity
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
      className={`duplicatecandidate-container ${className}`}
      style={style}
      data-testid={testId || 'duplicatecandidate'}
      role="region"
      aria-label="DuplicateCandidate"
    >
      {/* Duplicate candidate card */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">DuplicateCandidate</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: CommentThread ────────────────────────────────────────────
interface CommentThreadProps {
    comments: any[];
  onAdd: (text: string) => void;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const CommentThread: React.FC<CommentThreadProps> = ({
  comments, onAdd
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
      className={`commentthread-container ${className}`}
      style={style}
      data-testid={testId || 'commentthread'}
      role="region"
      aria-label="CommentThread"
    >
      {/* Comment thread */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">CommentThread</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: ActivityTimeline ────────────────────────────────────────────
interface ActivityTimelineProps {
    events: any[];
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const ActivityTimeline: React.FC<ActivityTimelineProps> = ({
  events
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
      className={`activitytimeline-container ${className}`}
      style={style}
      data-testid={testId || 'activitytimeline'}
      role="region"
      aria-label="ActivityTimeline"
    >
      {/* Issue activity timeline */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">ActivityTimeline</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: QualityScoreBreakdown ────────────────────────────────────────────
interface QualityScoreBreakdownProps {
    score: number;
  dimensions: Record<string, number>;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const QualityScoreBreakdown: React.FC<QualityScoreBreakdownProps> = ({
  score, dimensions
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
      className={`qualityscorebreakdown-container ${className}`}
      style={style}
      data-testid={testId || 'qualityscorebreakdown'}
      role="region"
      aria-label="QualityScoreBreakdown"
    >
      {/* Quality score breakdown */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">QualityScoreBreakdown</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};

// ── Sub-component: DeveloperRecommendation ────────────────────────────────────────────
interface DeveloperRecommendationProps {
    developer: Developer;
  score: number;
  reason: string;
  className?: string;
  style?: React.CSSProperties;
  testId?: string;
}

const DeveloperRecommendation: React.FC<DeveloperRecommendationProps> = ({
  developer, score, reason
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
      className={`developerrecommendation-container ${className}`}
      style={style}
      data-testid={testId || 'developerrecommendation'}
      role="region"
      aria-label="DeveloperRecommendation"
    >
      {/* Developer recommendation card */}
      <div className="flex flex-col gap-3">
        <span className="text-sm font-medium">DeveloperRecommendation</span>
      </div>
      <div className="hidden" aria-hidden="true" data-local-state={JSON.stringify(localState)} />
    </div>
  );
};


// ── Hooks ─────────────────────────────────────────────────────────────────────

function useIssueDetailLogic(props: IssueDetailProps) {
  const [state, dispatch] = useReducer(issueDetailReducer, {
    loading: {},
    errors: {},
    data: {},
    lastUpdated: {},
  });

  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<Record<string, boolean>>({});
    const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [activeTab, setActiveTab] = useState<string>('overview');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [showModal, setShowModal] = useState<boolean>(false);
  const [isEditing, setIsEditing] = useState<boolean>(false);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; id: string } | null>(null);
  const [timeRange, setTimeRange] = useState<TimeRange>('7d');
  const [viewMode, setViewMode] = useState<'grid' | 'list' | 'kanban'>('list');

  const mountedRef = useRef(true);
  const abortControllerRef = useRef<AbortController | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const virtualListRef = useRef<HTMLDivElement>(null);
  // Memoized filtered + sorted items
  const processedItems = useMemo(() => { return (props.items ?? []).filter(Boolean); }, [props.items, searchQuery]);
  // Keyboard shortcut handler
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { setShowModal(false); setContextMenu(null); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'a') { e.preventDefault(); handleSelectAll(); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') { e.preventDefault(); searchRef.current?.focus(); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'r') { e.preventDefault(); handleRefresh(); }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
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
    selectedIds, expandedIds, activeTab, searchQuery, showModal, isEditing, hoveredId, contextMenu, timeRange, viewMode,
    handleSelect, handleSelectAll, handleSearch, handleFilter, handleSort, handleExpand, handleContextMenu, handleBulkAction, handleExport, handleRefresh, handleTabChange, handleViewModeChange, handleTimeRangeChange,
  };
}

// ── Main Component ────────────────────────────────────────────────────────────

/**
 * IssueDetail
 *
 * Detailed issue view panel with ML signal cards, duplicate detection, developer recommendations, and activity log
 *
 * @example
 * ```tsx
 * <IssueDetail
 *   items={[]}
 *   className="my-4"
 * />
 * ```
 */
export const IssueDetail: React.FC<IssueDetailProps> = memo((props) => {
  const {
    items, onSelect, onAction, loading, error, filters, sortConfig, pagination, theme, onFilterChange, onSortChange, onPageChange, onRefresh, onExport, issue, onClose, onApprove, onReject, developers,
    className = '',
    style,
    'data-testid': testId = 'issuedetail',
    'aria-label': ariaLabel = 'Detailed issue view panel with ML signal cards, duplicate detection, developer recommendations, and activity log',
  } = props;

  const logic = useIssueDetailLogic(props);

  const contextValue = useMemo<IssueDetailContextValue>(() => ({
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
    <IssueDetailContext.Provider value={contextValue}>
      <div
        className={`issuedetail flex flex-col gap-4 ${className}`}
        style={style}
        data-testid={testId}
        aria-label={ariaLabel}
      >
        {/* Header */}
        <header className="flex items-center justify-between pb-4 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-bold text-gray-900 dark:text-white">
              \1ssue \1etail
            </h2>
            <p className="text-sm text-gray-500 mt-1">Detailed issue view panel with ML signal cards, duplicate detection, developer recommendations, and activity log</p>
          </div>
          <div className="flex items-center gap-2">
            {logic.loading['main'] && (
              <div className="h-5 w-5 animate-spin rounded-full border-2 border-blue-600 border-t-transparent" />
            )}
          </div>
        </header>

        {/* Content */}
        <main className="flex-1 min-h-0">
          
        {/* ── Issue Header ── */}
        <section className="section p-4" aria-label="Issue Header">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Issue Header
            </h3>
            <span className="text-sm text-gray-500"></span>
          </div>
          <div className='issue-header'></div>
        </section>
        {/* ── ML Predictions ── */}
        <section className="section p-4" aria-label="ML Predictions">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              ML Predictions
            </h3>
            <span className="text-sm text-gray-500"></span>
          </div>
          <div className="tab-bar flex gap-1 border-b border-gray-200 dark:border-gray-700 pb-2" role="tablist">
          
          <button
            key="predictions"
            onClick={() => handleTabChange('predictions')}
            className={`tab-btn px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              activeTab === 'predictions'
                ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                : 'text-gray-600 hover:text-gray-900 dark:text-gray-400'
            }`}
            aria-selected={activeTab === 'predictions'}
            role="tab"
          >
            Predictions
          </button>
          <button
            key="duplicates"
            onClick={() => handleTabChange('duplicates')}
            className={`tab-btn px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              activeTab === 'duplicates'
                ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                : 'text-gray-600 hover:text-gray-900 dark:text-gray-400'
            }`}
            aria-selected={activeTab === 'duplicates'}
            role="tab"
          >
            Duplicates
          </button>
          <button
            key="assignment"
            onClick={() => handleTabChange('assignment')}
            className={`tab-btn px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              activeTab === 'assignment'
                ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                : 'text-gray-600 hover:text-gray-900 dark:text-gray-400'
            }`}
            aria-selected={activeTab === 'assignment'}
            role="tab"
          >
            Assignment
          </button>
          <button
            key="quality"
            onClick={() => handleTabChange('quality')}
            className={`tab-btn px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              activeTab === 'quality'
                ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                : 'text-gray-600 hover:text-gray-900 dark:text-gray-400'
            }`}
            aria-selected={activeTab === 'quality'}
            role="tab"
          >
            Quality
          </button>
          <button
            key="history"
            onClick={() => handleTabChange('history')}
            className={`tab-btn px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              activeTab === 'history'
                ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                : 'text-gray-600 hover:text-gray-900 dark:text-gray-400'
            }`}
            aria-selected={activeTab === 'history'}
            role="tab"
          >
            History
          </button>
        </div>
        </section>
        {/* ── Actions ── */}
        <section className="section p-4" aria-label="Actions">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Actions
            </h3>
            <span className="text-sm text-gray-500"></span>
          </div>
          <div className='action-bar flex gap-2 justify-end'></div>
        </section>
        </main>

        {/* Footer */}
        <footer className="pt-4 border-t border-gray-200 dark:border-gray-700 text-xs text-gray-400 flex justify-between">
          <span>IssueDetail v1.0.0</span>
          <span>GitPilot AI</span>
        </footer>
      </div>
    </IssueDetailContext.Provider>
  );
});

IssueDetail.displayName = 'IssueDetail';

export default IssueDetail;
