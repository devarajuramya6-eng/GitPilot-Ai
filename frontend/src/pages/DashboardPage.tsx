import { useState } from 'react';
import { Link } from 'react-router-dom';
import {
  Cpu, Layers, Users, Activity, GitBranch, AlertTriangle,
  CheckCircle2, Clock, Bot, Sparkles, Sliders, ShieldCheck,
  Zap, Terminal, FolderGit2, Search, Bell, Settings, LogOut,
  BarChart3, TrendingUp, ShieldAlert, RefreshCw, Check, Database
} from 'lucide-react';
import { useAuthStore } from '../store/authStore';
import toast from 'react-hot-toast';

type TabType = 'control-room' | 'issues' | 'engineers' | 'health' | 'models';

const SAMPLE_ISSUES = [
  {
    id: 'iss-101', number: 145,
    title: 'JWT Token rotation fails intermittently under high concurrent requests',
    author: 'alex_dev', created: '2026-09-12 10:31',
    repo: 'auth-core-service', type: 'Security', priority: 'Critical',
    complexity: 'Hard', component: 'Authentication & Session',
    duplicateProb: 8, qualityScore: 92,
    labels: ['security', 'auth', 'high-concurrency'],
    recommendedDev: { name: 'Anil K.', score: 94, workload: 'Optimal', matchReason: 'Auth stack authority (24 merged PRs in auth-core), handled 12 similar concurrency issues' },
    explanation: ['Critical production impact detected from "token rotation fails" in auth pathway', 'High concurrency stress pattern matches Redis lock contention history', 'No close duplicates found in active issues (Cosine sim < 0.12)'],
    status: 'triage_pending', timestamp: '3 mins ago'
  },
  {
    id: 'iss-102', number: 146,
    title: 'Memory leak in streaming response handler when client disconnects early',
    author: 'david_ops', created: '2026-09-12 10:15',
    repo: 'gateway-api', type: 'Bug', priority: 'High',
    complexity: 'Hard', component: 'API Gateway / Network',
    duplicateProb: 84, qualityScore: 78,
    labels: ['bug', 'network', 'memory-leak'],
    recommendedDev: { name: 'Elena Rostova', score: 89, workload: 'Optimal', matchReason: 'Core maintainer for gateway SSE engine, 98% resolution velocity' },
    explanation: ['Duplicate candidate detected: 84% semantic similarity with #109 (resolved in v2.4.1)', 'High severity allocated due to worker container OOM kill risk'],
    status: 'triage_pending', timestamp: '18 mins ago'
  },
  {
    id: 'iss-103', number: 147,
    title: 'Implement Prometheus exporter for database connection pool statistics',
    author: 'sara_infra', created: '2026-09-12 09:45',
    repo: 'analytics-pipeline', type: 'Feature', priority: 'Medium',
    complexity: 'Medium', component: 'Observability & Metrics',
    duplicateProb: 4, qualityScore: 88,
    labels: ['feature', 'metrics', 'prometheus'],
    recommendedDev: { name: 'Marcus Vance', score: 86, workload: 'Light', matchReason: 'Authored metrics pipeline exporter modules, capacity available (1 open task)' },
    explanation: ['Tagged as Observability expansion based on metric/exporter semantic cluster', 'Developer workload balanced: Marcus has highest bandwidth among DevOps peers'],
    status: 'auto_approved', timestamp: '45 mins ago'
  },
  {
    id: 'iss-104', number: 148,
    title: 'CSS layout breaks on ultra-wide viewports in Repository Settings view',
    author: 'emily_ui', created: '2026-09-12 08:30',
    repo: 'console-ui', type: 'Bug', priority: 'Low',
    complexity: 'Easy', component: 'Frontend / UI',
    duplicateProb: 15, qualityScore: 64,
    labels: ['ui', 'bug', 'responsive'],
    recommendedDev: { name: 'Sarah Chen', score: 91, workload: 'Heavy', matchReason: 'Frontend lead, but currently overloaded (5 in-progress tickets)' },
    explanation: ['Low priority: Non-blocking visual inconsistency on rare viewport dimensions', 'Quality score flagged: missing screen captures and browser version details'],
    status: 'action_taken', timestamp: '2 hours ago'
  },
  {
    id: 'iss-105', number: 149,
    title: 'Sanitize user-controlled file path parameter in project archive unpacker',
    author: 'sec_bot', created: '2026-09-12 07:15',
    repo: 'auth-core-service', type: 'Security', priority: 'Critical',
    complexity: 'Hard', component: 'File Upload / Storage',
    duplicateProb: 6, qualityScore: 95,
    labels: ['security', 'zip-slip', 'cve-audit'],
    recommendedDev: { name: 'Anil K.', score: 96, workload: 'Optimal', matchReason: 'Authored project upload security guards & path normalization modules' },
    explanation: ['Zip Slip path traversal prevention required in archive handler', 'High severity CVE mitigation pattern applied'],
    status: 'triage_pending', timestamp: '3 hours ago'
  }
];

const DEVELOPERS_DATA = [
  {
    id: 'dev-1',
    name: 'Anil K.',
    role: 'Principal Security & Backend Engineer',
    avatar: 'AK',
    skills: ['Authentication', 'Security', 'Cryptography', 'Database', 'Python', 'FastAPI'],
    openIssues: 2,
    capacity: 6,
    workload: 'Optimal',
    workloadPercentage: 33,
    resolvedCount: 142,
    avgResolutionTime: '4.2 hrs',
    successRate: 96,
    specialty: 'Auth & Cryptography Core',
    recentActivity: 'Closed #142 (OAuth PKCE edge case) 4 hours ago'
  },
  {
    id: 'dev-2',
    name: 'Elena Rostova',
    role: 'Staff Distributed Systems Engineer',
    avatar: 'ER',
    skills: ['API Gateway', 'Go', 'Networking', 'Performance', 'gRPC', 'SSE'],
    openIssues: 3,
    capacity: 6,
    workload: 'Optimal',
    workloadPercentage: 50,
    resolvedCount: 188,
    avgResolutionTime: '3.8 hrs',
    successRate: 98,
    specialty: 'High-Throughput Gateway & SSE Streams',
    recentActivity: 'Merged PR #89 (Fix worker socket leak) yesterday'
  },
  {
    id: 'dev-3',
    name: 'Marcus Vance',
    role: 'Senior DevOps & Observability Engineer',
    avatar: 'MV',
    skills: ['Observability', 'Prometheus', 'Docker', 'Kubernetes', 'Python', 'PostgreSQL'],
    openIssues: 1,
    capacity: 5,
    workload: 'Light',
    workloadPercentage: 20,
    resolvedCount: 94,
    avgResolutionTime: '5.1 hrs',
    successRate: 92,
    specialty: 'Metrics Pipelines & DB Optimization',
    recentActivity: 'Optimized Prometheus scrape intervals 2 days ago'
  },
  {
    id: 'dev-4',
    name: 'Sarah Chen',
    role: 'Lead Frontend & Design Systems Architect',
    avatar: 'SC',
    skills: ['Frontend', 'React', 'TypeScript', 'Tailwind CSS', 'UI/UX', 'Accessibility'],
    openIssues: 5,
    capacity: 5,
    workload: 'Heavy',
    workloadPercentage: 100,
    resolvedCount: 210,
    avgResolutionTime: '2.9 hrs',
    successRate: 94,
    specialty: 'Design System & Complex UI State',
    recentActivity: 'Pushed commit to console-ui layout engine 30 mins ago'
  }
];

const ML_MODELS_DATA = [
  {
    id: 'mod-1',
    name: 'Issue Type Classifier',
    architecture: 'TF-IDF + Linear SVM Ensemble',
    version: 'v2.4.1 (Local)',
    datasetSize: '12,450 historical issues',
    accuracy: 94.2,
    precision: 93.8,
    recall: 94.5,
    f1Score: 94.1,
    trainingDate: '2026-09-10',
    status: 'Active (Production)',
    latency: '11ms',
    features: ['Token n-grams (1-3)', 'Technical signal lexicon', 'Component indicators', 'Code snippet presence']
  },
  {
    id: 'mod-2',
    name: 'Priority & Severity Predictor',
    architecture: 'Gradient Boosted Decision Forest',
    version: 'v1.8.0 (Local)',
    datasetSize: '8,200 labeled reports',
    accuracy: 91.5,
    precision: 90.9,
    recall: 92.1,
    f1Score: 91.4,
    trainingDate: '2026-09-08',
    status: 'Active (Production)',
    latency: '8ms',
    features: ['Crash indicators', 'Production environment tags', 'Error stack trace patterns', 'User impact severity']
  },
  {
    id: 'mod-3',
    name: 'Duplicate Detection Engine',
    architecture: 'TF-IDF Vector Space + Cosine Similarity',
    version: 'v3.1.2 (Local)',
    datasetSize: '24,800 indexed corpus items',
    accuracy: 88.4,
    precision: 89.2,
    recall: 87.6,
    f1Score: 88.3,
    trainingDate: '2026-09-11',
    status: 'Active (Production)',
    latency: '14ms',
    features: ['Sub-string character n-grams', 'Cosine angular distance', 'Vocabulary overlap', 'Error message fingerprints']
  },
  {
    id: 'mod-4',
    name: 'Developer Suitability Engine',
    architecture: 'Multi-Factor Heuristic & Skill-Workload Balancer',
    version: 'v2.0.0 (Local)',
    datasetSize: '1,420 historical PRs & assignments',
    accuracy: 96.1,
    precision: 95.8,
    recall: 96.4,
    f1Score: 96.0,
    trainingDate: '2026-09-12',
    status: 'Active (Production)',
    latency: '4ms',
    features: ['Component expertise weight', 'Workload capacity penalty', 'Past resolution speed', 'Historical regression rate']
  }
];

export default function DashboardPage() {
  const { user, logout } = useAuthStore();
  const [activeTab, setActiveTab] = useState<TabType>('control-room');
  const [selected, setSelected] = useState(SAMPLE_ISSUES[0]);
  const [issues, setIssues] = useState(SAMPLE_ISSUES);
  const [search, setSearch] = useState('');
  const [selectedTypeFilter, setSelectedTypeFilter] = useState('All');
  const [isRetraining, setIsRetraining] = useState(false);

  const handleApprove = (id: string) => {
    setIssues(prev => prev.map(i => i.id === id ? { ...i, status: 'auto_approved' } : i));
    if (selected.id === id) setSelected(prev => ({ ...prev, status: 'auto_approved' }));
    toast.success(`Issue #${selected.number} triaged & assigned to ${selected.recommendedDev.name} on GitHub`);
  };

  const handleRetrain = (modelName: string) => {
    setIsRetraining(true);
    setTimeout(() => {
      setIsRetraining(false);
      toast.success(`${modelName} retrained on latest local corpus! Accuracy +0.3%`);
    }, 1500);
  };

  const priorityBadge = (p: string) =>
    p === 'Critical' ? 'bg-red-950 text-red-400 border-red-800/60'
    : p === 'High' ? 'bg-amber-950 text-amber-400 border-amber-800/60'
    : p === 'Medium' ? 'bg-blue-950 text-blue-400 border-blue-800/60'
    : 'bg-slate-900 text-slate-400 border-slate-700';

  return (
    <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9] font-sans flex flex-col">

      {/* ── Top Header & Tab Navigation ── */}
      <header className="border-b border-[#30363d] bg-[#161b22] px-5 py-2.5 flex items-center justify-between sticky top-0 z-50">
        <div className="flex items-center space-x-3">
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
            <Cpu className="h-4 w-4 text-white" />
          </div>
          <div>
            <span className="font-bold text-white text-sm tracking-wide">GitPilot AI</span>
            <span className="ml-2 px-1.5 py-0.5 rounded text-[10px] font-mono font-medium bg-cyan-950 text-cyan-400 border border-cyan-800/60">LOCAL ENGINE</span>
          </div>
        </div>

        {/* Dynamic Tab Switcher */}
        <nav className="flex items-center space-x-1 bg-[#0d1117] px-1 py-1 rounded-lg border border-[#30363d]">
          {[
            { id: 'control-room', label: 'Control Room', icon: Terminal },
            { id: 'issues', label: 'Issues', icon: Layers },
            { id: 'engineers', label: 'Engineers', icon: Users },
            { id: 'health', label: 'Health', icon: Activity },
            { id: 'models', label: 'ML Models', icon: Cpu },
          ].map(tab => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as TabType)}
                className={`flex items-center space-x-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                  isActive
                    ? 'bg-[#21262d] text-white border border-[#30363d] shadow-sm'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                <Icon className={`h-3.5 w-3.5 ${isActive ? 'text-cyan-400' : 'text-slate-500'}`} />
                <span>{tab.label}</span>
              </button>
            );
          })}
        </nav>

        {/* System telemetry & user actions */}
        <div className="flex items-center space-x-3">
          <div className="flex items-center space-x-2 bg-[#21262d] px-2.5 py-1 rounded border border-[#30363d] text-xs font-mono">
            <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
            <span className="text-slate-300">Inference: 18ms</span>
          </div>
          <button className="text-slate-400 hover:text-white transition-colors p-1.5"><Bell className="h-4 w-4" /></button>
          <Link to="/connect" title="Connect / Upload Project" className="text-slate-400 hover:text-white transition-colors p-1.5"><FolderGit2 className="h-4 w-4" /></Link>
          <button className="text-slate-400 hover:text-white transition-colors p-1.5"><Settings className="h-4 w-4" /></button>
          <button onClick={() => { logout(); toast('Signed out'); }} className="text-slate-400 hover:text-rose-400 transition-colors p-1.5"><LogOut className="h-4 w-4" /></button>
          <div className="h-7 w-7 rounded-full bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-xs font-bold text-white">
            {user?.email?.substring(0, 2).toUpperCase() || 'U'}
          </div>
        </div>
      </header>

      {/* ── View Container: Switches according to activeTab ── */}
      <div className="flex-1 flex overflow-hidden">

        {/* ══════════════════════════════════════════════════════
            TAB 1: CONTROL ROOM (Default Master View)
        ══════════════════════════════════════════════════════ */}
        {activeTab === 'control-room' && (
          <div className="flex-1 flex overflow-hidden">
            {/* Left Column: Triage Stream List */}
            <div className="w-[380px] border-r border-[#30363d] bg-[#161b22]/30 flex flex-col flex-shrink-0">
              <div className="p-4 border-b border-[#30363d]">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-xs font-semibold uppercase tracking-wider text-slate-400 flex items-center gap-1.5">
                    <Layers className="h-3.5 w-3.5 text-cyan-400" />
                    Triage Stream
                  </span>
                  <span className="text-xs font-mono bg-cyan-950 text-cyan-400 px-2 py-0.5 rounded border border-cyan-800/60">
                    {issues.length} signals
                  </span>
                </div>
                <div className="relative">
                  <Search className="h-3.5 w-3.5 absolute left-3 top-2.5 text-slate-500" />
                  <input
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    placeholder="Search issues..."
                    className="w-full bg-[#0d1117] border border-[#30363d] rounded-md pl-9 pr-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition-colors font-mono"
                  />
                </div>
              </div>

              <div className="flex-1 overflow-y-auto divide-y divide-[#21262d]">
                {issues
                  .filter(i => i.title.toLowerCase().includes(search.toLowerCase()) || i.component.toLowerCase().includes(search.toLowerCase()))
                  .map(issue => (
                    <div
                      key={issue.id}
                      onClick={() => setSelected(issue)}
                      className={`p-4 cursor-pointer transition-all ${
                        selected.id === issue.id
                          ? 'bg-[#1c2128] border-l-4 border-cyan-400'
                          : 'hover:bg-[#161b22] border-l-4 border-transparent'
                      }`}
                    >
                      <div className="flex items-center justify-between mb-1.5">
                        <span className="text-xs font-mono text-cyan-400 font-medium">#{issue.number}</span>
                        <span className={`text-[10px] px-2 py-0.5 rounded font-mono font-medium border ${priorityBadge(issue.priority)}`}>
                          {issue.priority}
                        </span>
                      </div>
                      <h4 className="text-xs font-medium text-slate-200 line-clamp-2 leading-relaxed mb-2">{issue.title}</h4>
                      <div className="flex items-center justify-between text-[11px] text-slate-400">
                        <span className="truncate max-w-[180px]">{issue.component}</span>
                        <div className="flex items-center gap-2">
                          {issue.duplicateProb > 50 && (
                            <span className="text-[10px] font-mono text-amber-400 bg-amber-950/80 px-1.5 py-0.5 rounded border border-amber-800/40">
                              {issue.duplicateProb}% dup
                            </span>
                          )}
                          <span className="font-mono text-[10px] text-slate-500">{issue.timestamp}</span>
                        </div>
                      </div>
                    </div>
                  ))}
              </div>

              <div className="p-3 border-t border-[#30363d] bg-[#161b22] flex items-center justify-between text-[11px] font-mono text-slate-400">
                <span className="flex items-center gap-1.5"><Activity className="h-3.5 w-3.5 text-cyan-400" />Queue: Normal</span>
                <span className="text-slate-500">Auto-triage: On</span>
              </div>
            </div>

            {/* Right Column: Selected Issue Inspection */}
            <div className="flex-1 flex flex-col overflow-y-auto">
              {/* Issue Header */}
              <div className="p-5 border-b border-[#30363d] bg-[#161b22]/30">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center space-x-3">
                    <span className="px-2.5 py-1 rounded bg-[#21262d] text-cyan-400 font-mono font-bold text-sm border border-[#30363d]">
                      ISSUE #{selected.number}
                    </span>
                    <span className="text-xs text-slate-400 font-mono">{selected.repo}</span>
                  </div>
                  <button
                    onClick={() => handleApprove(selected.id)}
                    disabled={selected.status === 'auto_approved'}
                    className={`px-3 py-1.5 rounded text-xs font-semibold flex items-center space-x-1.5 transition-all ${
                      selected.status === 'auto_approved'
                        ? 'bg-emerald-950 text-emerald-400 border border-emerald-800 cursor-default'
                        : 'bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white shadow-md shadow-cyan-600/20'
                    }`}
                  >
                    <CheckCircle2 className="h-3.5 w-3.5" />
                    <span>{selected.status === 'auto_approved' ? 'Applied to GitHub' : 'Apply Decisions to GitHub'}</span>
                  </button>
                </div>
                <h1 className="text-base font-semibold text-white mb-4 leading-snug">{selected.title}</h1>
                <div className="grid grid-cols-5 gap-2 font-mono">
                  {[
                    { label: 'Issue Class', value: selected.type, icon: Bot, color: 'text-cyan-400' },
                    { label: 'Priority', value: selected.priority, icon: AlertTriangle, color: 'text-amber-400' },
                    { label: 'Complexity', value: selected.complexity, icon: Sliders, color: 'text-purple-400' },
                    { label: 'Dup Risk', value: `${selected.duplicateProb}%`, icon: GitBranch, color: selected.duplicateProb > 40 ? 'text-amber-400' : 'text-emerald-400' },
                    { label: 'Quality', value: `${selected.qualityScore}/100`, icon: Sparkles, color: 'text-cyan-400' },
                  ].map(sig => {
                    const Icon = sig.icon;
                    return (
                      <div key={sig.label} className="bg-[#161b22] p-2.5 rounded border border-[#30363d]">
                        <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{sig.label}</div>
                        <div className={`text-xs font-semibold ${sig.color} flex items-center gap-1.5`}>
                          <Icon className="h-3.5 w-3.5" />{sig.value}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Developer Recommendation & Explainable AI */}
              <div className="p-5 grid grid-cols-2 gap-5">
                <div className="bg-[#161b22] p-5 rounded-lg border border-[#30363d]">
                  <div className="flex items-center justify-between mb-4 border-b border-[#30363d] pb-3">
                    <span className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center gap-2 font-mono">
                      <Users className="h-4 w-4 text-cyan-400" />Developer Match
                    </span>
                    <span className="text-xs font-mono text-emerald-400 bg-emerald-950 px-2 py-0.5 rounded border border-emerald-800">
                      {selected.recommendedDev.score}% Confidence
                    </span>
                  </div>
                  <div className="flex items-start space-x-3 mb-4">
                    <div className="h-10 w-10 rounded-full bg-gradient-to-tr from-cyan-600 to-emerald-600 flex items-center justify-center font-bold text-white text-sm shadow-md">
                      {selected.recommendedDev.name.substring(0, 2).toUpperCase()}
                    </div>
                    <div>
                      <h3 className="font-semibold text-white text-sm">{selected.recommendedDev.name}</h3>
                      <div className="text-xs font-mono text-slate-400">
                        Workload: <strong className="text-emerald-400">{selected.recommendedDev.workload}</strong>
                      </div>
                    </div>
                  </div>
                  <div className="bg-[#0d1117] p-3 rounded border border-[#30363d]">
                    <div className="text-[11px] font-mono text-slate-400 uppercase tracking-wide mb-1">Rationale</div>
                    <p className="text-xs text-slate-300 leading-relaxed">{selected.recommendedDev.matchReason}</p>
                  </div>
                  <div className="text-xs text-slate-400 flex items-center justify-between border-t border-[#30363d] pt-3 mt-3 font-mono">
                    <span>Workload Balancer</span>
                    <span className="text-emerald-400 flex items-center gap-1">
                      <ShieldCheck className="h-3.5 w-3.5" />Safe Quota
                    </span>
                  </div>
                </div>

                <div className="bg-[#161b22] p-5 rounded-lg border border-[#30363d] flex flex-col">
                  <div className="flex items-center justify-between mb-4 border-b border-[#30363d] pb-3">
                    <span className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center gap-2 font-mono">
                      <Zap className="h-4 w-4 text-amber-400" />AI Rationale
                    </span>
                    <span className="text-xs font-mono text-slate-400">Local Features</span>
                  </div>
                  <ul className="space-y-2.5 flex-1">
                    {selected.explanation.map((item, idx) => (
                      <li key={idx} className="flex items-start space-x-2.5 text-xs text-slate-300 leading-relaxed">
                        <CheckCircle2 className="h-4 w-4 text-cyan-400 flex-shrink-0 mt-0.5" />
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                  <div className="mt-4 pt-3 border-t border-[#30363d] bg-[#0d1117]/60 p-2.5 rounded text-[11px] font-mono text-slate-400">
                    <span className="text-slate-300 font-semibold">Local ML:</span> 1,420 historical issues trained. Zero external API calls.
                  </div>
                </div>
              </div>

              {/* Lower Intelligence Signals */}
              <div className="px-5 pb-5 grid grid-cols-3 gap-4">
                <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
                  <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2">
                    <BarChart3 className="h-3.5 w-3.5 text-cyan-400" />Repo Health
                  </div>
                  <div className="space-y-2 text-xs font-mono">
                    <div className="flex justify-between text-slate-400"><span>Issue Resolution</span><span className="text-slate-200">82/100</span></div>
                    <div className="flex justify-between text-slate-400"><span>Duplicate Prevention</span><span className="text-slate-200">94/100</span></div>
                    <div className="flex justify-between text-slate-400"><span>Response Velocity</span><span className="text-slate-200">88/100</span></div>
                  </div>
                  <div className="mt-3 pt-3 border-t border-[#30363d] flex items-center justify-between text-xs font-mono">
                    <span className="text-slate-400">Overall</span>
                    <span className="text-emerald-400 font-bold">84/100</span>
                  </div>
                </div>

                <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
                  <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2">
                    <Clock className="h-3.5 w-3.5 text-emerald-400" />Repository Memory
                  </div>
                  <div className="text-xs text-slate-300 space-y-1.5 font-mono">
                    <p>Auth MTTR: <strong className="text-white">4.2 hours</strong></p>
                    <p>Resolution rate: <strong className="text-emerald-400">91%</strong></p>
                    <p>Top resolver: <strong className="text-white">Anil K.</strong></p>
                  </div>
                </div>

                <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
                  <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2">
                    <TrendingUp className="h-3.5 w-3.5 text-purple-400" />Triage Metrics
                  </div>
                  <div className="text-xs text-slate-300 space-y-1.5 font-mono">
                    <p>Avg inference: <strong className="text-white">18ms</strong></p>
                    <p>Auto-approved: <strong className="text-emerald-400">12</strong></p>
                    <p>Accuracy: <strong className="text-cyan-400">94.2%</strong></p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ══════════════════════════════════════════════════════
            TAB 2: ISSUES (Full Triage Queue & Filterable Table)
        ══════════════════════════════════════════════════════ */}
        {activeTab === 'issues' && (
          <div className="flex-1 bg-[#0d1117] flex flex-col p-6 overflow-y-auto">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h2 className="text-xl font-bold text-white tracking-wide">Repository Issues & Triage Queue</h2>
                <p className="text-xs text-slate-400 font-mono mt-1">
                  Full list of open issues with local ML triage signals, duplicate probability, and developer match scores.
                </p>
              </div>
              <div className="flex items-center space-x-3">
                <span className="text-xs font-mono text-slate-400 bg-[#161b22] px-3 py-1.5 rounded border border-[#30363d]">
                  {issues.length} Total Issues
                </span>
                <button
                  onClick={() => toast.success('Synchronized with connected repositories')}
                  className="flex items-center space-x-1.5 px-3 py-1.5 rounded text-xs font-medium bg-[#21262d] hover:bg-[#30363d] border border-[#30363d] text-slate-200 transition-colors"
                >
                  <RefreshCw className="h-3.5 w-3.5 text-cyan-400" />
                  <span>Sync Repository</span>
                </button>
              </div>
            </div>

            {/* Filter Bar */}
            <div className="flex items-center space-x-3 mb-4">
              <div className="relative flex-1">
                <Search className="h-4 w-4 absolute left-3 top-2.5 text-slate-500" />
                <input
                  type="text"
                  value={search}
                  onChange={e => setSearch(e.target.value)}
                  placeholder="Filter by title, component, or author..."
                  className="w-full bg-[#161b22] border border-[#30363d] rounded-lg pl-9 pr-4 py-2 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500 font-mono"
                />
              </div>

              <div className="flex items-center space-x-1 bg-[#161b22] p-1 rounded-lg border border-[#30363d]">
                {['All', 'Bug', 'Security', 'Feature'].map(type => (
                  <button
                    key={type}
                    onClick={() => setSelectedTypeFilter(type)}
                    className={`px-3 py-1 text-xs rounded-md font-mono transition-all ${
                      selectedTypeFilter === type
                        ? 'bg-[#21262d] text-cyan-400 border border-[#30363d]'
                        : 'text-slate-400 hover:text-white'
                    }`}
                  >
                    {type}
                  </button>
                ))}
              </div>
            </div>

            {/* Issues Table */}
            <div className="bg-[#161b22] rounded-xl border border-[#30363d] overflow-hidden">
              <table className="w-full text-left text-xs">
                <thead className="bg-[#0d1117] border-b border-[#30363d] text-[11px] font-mono text-slate-400 uppercase tracking-wider">
                  <tr>
                    <th className="py-3 px-4">Issue</th>
                    <th className="py-3 px-4">Type</th>
                    <th className="py-3 px-4">Priority</th>
                    <th className="py-3 px-4">Component</th>
                    <th className="py-3 px-4">Duplicate Risk</th>
                    <th className="py-3 px-4">Recommended Dev</th>
                    <th className="py-3 px-4 text-right">Status / Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#21262d] font-mono">
                  {issues
                    .filter(i => selectedTypeFilter === 'All' || i.type === selectedTypeFilter)
                    .filter(i => i.title.toLowerCase().includes(search.toLowerCase()) || i.component.toLowerCase().includes(search.toLowerCase()))
                    .map(issue => (
                      <tr key={issue.id} className="hover:bg-[#1c2128] transition-colors">
                        <td className="py-3.5 px-4">
                          <div className="flex items-center space-x-2">
                            <span className="text-cyan-400 font-semibold">#{issue.number}</span>
                            <span className="text-white font-sans font-medium line-clamp-1 max-w-[280px]">{issue.title}</span>
                          </div>
                          <span className="text-[10px] text-slate-500 font-mono">by {issue.author} · {issue.repo}</span>
                        </td>
                        <td className="py-3.5 px-4 text-slate-300">{issue.type}</td>
                        <td className="py-3.5 px-4">
                          <span className={`px-2 py-0.5 rounded text-[10px] border ${priorityBadge(issue.priority)}`}>
                            {issue.priority}
                          </span>
                        </td>
                        <td className="py-3.5 px-4 text-slate-400">{issue.component}</td>
                        <td className="py-3.5 px-4">
                          <span className={issue.duplicateProb > 40 ? 'text-amber-400' : 'text-emerald-400'}>
                            {issue.duplicateProb}%
                          </span>
                        </td>
                        <td className="py-3.5 px-4">
                          <div className="text-slate-200">{issue.recommendedDev.name}</div>
                          <span className="text-[10px] text-emerald-400">{issue.recommendedDev.score}% match</span>
                        </td>
                        <td className="py-3.5 px-4 text-right">
                          {issue.status === 'auto_approved' ? (
                            <span className="inline-flex items-center space-x-1 text-emerald-400 bg-emerald-950/80 px-2 py-1 rounded border border-emerald-800/60 text-[10px]">
                              <Check className="h-3 w-3" />
                              <span>Triaged</span>
                            </span>
                          ) : (
                            <button
                              onClick={() => { setSelected(issue); setActiveTab('control-room'); }}
                              className="px-2.5 py-1 rounded bg-[#21262d] hover:bg-cyan-950 text-slate-300 hover:text-cyan-400 border border-[#30363d] text-[11px] transition-colors"
                            >
                              Inspect AI
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ══════════════════════════════════════════════════════
            TAB 3: ENGINEERS (Developer Matching & Workload Balancer)
        ══════════════════════════════════════════════════════ */}
        {activeTab === 'engineers' && (
          <div className="flex-1 bg-[#0d1117] flex flex-col p-6 overflow-y-auto">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h2 className="text-xl font-bold text-white tracking-wide">Developer Profiles & Workload Balancer</h2>
                <p className="text-xs text-slate-400 font-mono mt-1">
                  Automated profile modeling from historical merged PRs, component ownership, and real-time active task loads.
                </p>
              </div>
              <div className="flex items-center space-x-2 text-xs font-mono text-emerald-400 bg-emerald-950 px-3 py-1.5 rounded border border-emerald-800/60">
                <ShieldCheck className="h-4 w-4" />
                <span>Workload Balancer Guardrail: Active</span>
              </div>
            </div>

            {/* Developer Grid */}
            <div className="grid grid-cols-2 gap-5">
              {DEVELOPERS_DATA.map(dev => (
                <div key={dev.id} className="bg-[#161b22] border border-[#30363d] rounded-xl p-5 hover:border-[#484f58] transition-all">
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex items-center space-x-3">
                      <div className="h-11 w-11 rounded-xl bg-gradient-to-tr from-cyan-600 to-blue-600 flex items-center justify-center font-bold text-white text-base shadow-lg">
                        {dev.avatar}
                      </div>
                      <div>
                        <h3 className="font-semibold text-white text-sm">{dev.name}</h3>
                        <p className="text-xs text-slate-400 font-mono">{dev.role}</p>
                      </div>
                    </div>
                    <span className={`text-[10px] font-mono px-2 py-0.5 rounded border ${
                      dev.workload === 'Optimal' ? 'bg-emerald-950 text-emerald-400 border-emerald-800' :
                      dev.workload === 'Light' ? 'bg-blue-950 text-blue-400 border-blue-800' :
                      'bg-amber-950 text-amber-400 border-amber-800'
                    }`}>
                      {dev.workload} ({dev.openIssues}/{dev.capacity} tasks)
                    </span>
                  </div>

                  {/* Workload bar */}
                  <div className="mb-4">
                    <div className="flex justify-between text-[11px] font-mono text-slate-400 mb-1">
                      <span>Capacity Allocation</span>
                      <span>{dev.workloadPercentage}%</span>
                    </div>
                    <div className="h-1.5 w-full bg-[#0d1117] rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${
                          dev.workloadPercentage > 80 ? 'bg-amber-400' : 'bg-cyan-400'
                        }`}
                        style={{ width: `${dev.workloadPercentage}%` }}
                      />
                    </div>
                  </div>

                  {/* Skills tags */}
                  <div className="mb-4">
                    <div className="text-[10px] uppercase font-mono text-slate-500 mb-1.5">Component Specialties</div>
                    <div className="flex flex-wrap gap-1.5">
                      {dev.skills.map(sk => (
                        <span key={sk} className="text-[11px] font-mono bg-[#0d1117] border border-[#30363d] px-2 py-0.5 rounded text-slate-300">
                          {sk}
                        </span>
                      ))}
                    </div>
                  </div>

                  {/* Stats footer */}
                  <div className="border-t border-[#30363d] pt-3 grid grid-cols-3 gap-2 text-center text-xs font-mono">
                    <div className="bg-[#0d1117] p-2 rounded">
                      <div className="text-[10px] text-slate-500">Resolved</div>
                      <div className="text-white font-semibold">{dev.resolvedCount}</div>
                    </div>
                    <div className="bg-[#0d1117] p-2 rounded">
                      <div className="text-[10px] text-slate-500">Avg MTTR</div>
                      <div className="text-white font-semibold">{dev.avgResolutionTime}</div>
                    </div>
                    <div className="bg-[#0d1117] p-2 rounded">
                      <div className="text-[10px] text-slate-500">Success Rate</div>
                      <div className="text-emerald-400 font-semibold">{dev.successRate}%</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ══════════════════════════════════════════════════════
            TAB 4: HEALTH (Engineering Health & Repository Memory)
        ══════════════════════════════════════════════════════ */}
        {activeTab === 'health' && (
          <div className="flex-1 bg-[#0d1117] flex flex-col p-6 overflow-y-auto">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h2 className="text-xl font-bold text-white tracking-wide">Repository Engineering Health & Memory</h2>
                <p className="text-xs text-slate-400 font-mono mt-1">
                  Continuous multi-vector health diagnostic synthesized from issue closure rates, duplicate prevention, and response times.
                </p>
              </div>
              <div className="flex items-center space-x-2 bg-[#161b22] px-3 py-1.5 rounded-lg border border-[#30363d]">
                <span className="text-xs font-mono text-slate-400">Overall Score:</span>
                <span className="text-sm font-bold font-mono text-emerald-400">84 / 100</span>
              </div>
            </div>

            {/* Health Pillars Grid */}
            <div className="grid grid-cols-3 gap-5 mb-6">
              {[
                { title: 'Issue Resolution Velocity', score: 82, status: 'Healthy', note: 'Median MTTR: 4.2h across all services', icon: Clock, color: 'text-cyan-400' },
                { title: 'Duplicate Prevention Rate', score: 94, status: 'Optimal', note: '18 potential duplicate issues averted', icon: GitBranch, color: 'text-emerald-400' },
                { title: 'Triage Response Time', score: 88, status: 'Optimal', note: 'Avg. automated triage latency: 18ms', icon: Zap, color: 'text-amber-400' },
                { title: 'Developer Load Balance', score: 69, status: 'Attention', note: 'Sarah Chen capacity overloaded (100%)', icon: Users, color: 'text-rose-400' },
                { title: 'Security Issue Hygiene', score: 91, status: 'Optimal', note: 'Zero unresolved Critical CVEs > 24h', icon: ShieldAlert, color: 'text-purple-400' },
                { title: 'Issue Quality Score', score: 78, status: 'Good', note: '82% of submitted issues have reproduction steps', icon: Sparkles, color: 'text-cyan-400' },
              ].map(pillar => {
                const Icon = pillar.icon;
                return (
                  <div key={pillar.title} className="bg-[#161b22] border border-[#30363d] rounded-xl p-5 flex flex-col justify-between">
                    <div>
                      <div className="flex items-center justify-between mb-3">
                        <Icon className={`h-5 w-5 ${pillar.color}`} />
                        <span className={`text-[10px] font-mono px-2 py-0.5 rounded border ${
                          pillar.status === 'Optimal' ? 'bg-emerald-950 text-emerald-400 border-emerald-800' :
                          pillar.status === 'Healthy' || pillar.status === 'Good' ? 'bg-blue-950 text-blue-400 border-blue-800' :
                          'bg-rose-950 text-rose-400 border-rose-800'
                        }`}>
                          {pillar.status}
                        </span>
                      </div>
                      <h3 className="font-semibold text-white text-sm mb-1">{pillar.title}</h3>
                      <p className="text-xs text-slate-400 font-mono">{pillar.note}</p>
                    </div>

                    <div className="mt-4 pt-3 border-t border-[#30363d]">
                      <div className="flex justify-between text-xs font-mono mb-1.5">
                        <span className="text-slate-400">Score</span>
                        <span className="text-white font-bold">{pillar.score}/100</span>
                      </div>
                      <div className="h-1.5 w-full bg-[#0d1117] rounded-full overflow-hidden">
                        <div className="h-full bg-cyan-400 rounded-full" style={{ width: `${pillar.score}%` }} />
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* Repository Memory Chronicle */}
            <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-5">
              <h3 className="text-sm font-bold uppercase tracking-wider text-slate-300 font-mono mb-3 flex items-center gap-2">
                <Database className="h-4 w-4 text-cyan-400" />
                Repository Memory Engine Chronicle
              </h3>
              <div className="space-y-2 text-xs font-mono text-slate-300">
                <div className="bg-[#0d1117] p-3 rounded border border-[#30363d] flex items-center justify-between">
                  <span><strong>Historical Pattern:</strong> Authentication bugs assigned to Anil K. achieve 96% first-pass resolution rate.</span>
                  <span className="text-emerald-400">Validated</span>
                </div>
                <div className="bg-[#0d1117] p-3 rounded border border-[#30363d] flex items-center justify-between">
                  <span><strong>Regression Alert:</strong> SSE client disconnection in gateway-api recurred after v2.4.1 fix.</span>
                  <span className="text-amber-400">Monitored</span>
                </div>
                <div className="bg-[#0d1117] p-3 rounded border border-[#30363d] flex items-center justify-between">
                  <span><strong>Workload Insight:</strong> Frontend component queue exceeds bandwidth during release cycles.</span>
                  <span className="text-cyan-400">Rebalanced</span>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ══════════════════════════════════════════════════════
            TAB 5: ML MODELS (Local Model Performance & Retraining)
        ══════════════════════════════════════════════════════ */}
        {activeTab === 'models' && (
          <div className="flex-1 bg-[#0d1117] flex flex-col p-6 overflow-y-auto">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h2 className="text-xl font-bold text-white tracking-wide">Local Machine Learning Pipeline</h2>
                <p className="text-xs text-slate-400 font-mono mt-1">
                  Classical NLP, TF-IDF, SVM, and heuristic decision models executed 100% locally. Zero external AI APIs.
                </p>
              </div>
              <div className="flex items-center space-x-2 text-xs font-mono text-cyan-400 bg-cyan-950 px-3 py-1.5 rounded border border-cyan-800/60">
                <ShieldCheck className="h-4 w-4" />
                <span>Zero External API Tokens Required</span>
              </div>
            </div>

            {/* Models List */}
            <div className="space-y-4">
              {ML_MODELS_DATA.map(mod => (
                <div key={mod.id} className="bg-[#161b22] border border-[#30363d] rounded-xl p-5">
                  <div className="flex items-start justify-between mb-3">
                    <div>
                      <div className="flex items-center space-x-2 mb-1">
                        <h3 className="font-semibold text-white text-base">{mod.name}</h3>
                        <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-[#0d1117] text-cyan-400 border border-[#30363d]">
                          {mod.version}
                        </span>
                      </div>
                      <p className="text-xs text-slate-400 font-mono">{mod.architecture} · Trained on {mod.datasetSize}</p>
                    </div>

                    <button
                      onClick={() => handleRetrain(mod.name)}
                      disabled={isRetraining}
                      className="flex items-center space-x-1.5 px-3 py-1.5 rounded text-xs font-medium bg-[#21262d] hover:bg-cyan-950 text-slate-300 hover:text-cyan-400 border border-[#30363d] transition-colors"
                    >
                      <RefreshCw className={`h-3.5 w-3.5 ${isRetraining ? 'animate-spin' : ''}`} />
                      <span>{isRetraining ? 'Retraining...' : 'Retrain on Local Corpus'}</span>
                    </button>
                  </div>

                  {/* Performance Metrics Bar */}
                  <div className="grid grid-cols-5 gap-3 py-3 border-y border-[#30363d] my-3 font-mono text-center">
                    <div className="bg-[#0d1117] p-2.5 rounded">
                      <div className="text-[10px] text-slate-500 uppercase">Accuracy</div>
                      <div className="text-sm font-bold text-white">{mod.accuracy}%</div>
                    </div>
                    <div className="bg-[#0d1117] p-2.5 rounded">
                      <div className="text-[10px] text-slate-500 uppercase">Precision</div>
                      <div className="text-sm font-bold text-white">{mod.precision}%</div>
                    </div>
                    <div className="bg-[#0d1117] p-2.5 rounded">
                      <div className="text-[10px] text-slate-500 uppercase">Recall</div>
                      <div className="text-sm font-bold text-white">{mod.recall}%</div>
                    </div>
                    <div className="bg-[#0d1117] p-2.5 rounded">
                      <div className="text-[10px] text-slate-500 uppercase">F1-Score</div>
                      <div className="text-sm font-bold text-emerald-400">{mod.f1Score}%</div>
                    </div>
                    <div className="bg-[#0d1117] p-2.5 rounded">
                      <div className="text-[10px] text-slate-500 uppercase">Avg Latency</div>
                      <div className="text-sm font-bold text-cyan-400">{mod.latency}</div>
                    </div>
                  </div>

                  {/* Feature Extractor Elements */}
                  <div>
                    <div className="text-[10px] uppercase font-mono text-slate-500 mb-1.5">Extracted Feature Signals</div>
                    <div className="flex flex-wrap gap-1.5">
                      {mod.features.map(f => (
                        <span key={f} className="text-[11px] font-mono bg-[#0d1117] border border-[#30363d] px-2 py-0.5 rounded text-slate-400">
                          {f}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

      </div>
    </div>
  );
}
