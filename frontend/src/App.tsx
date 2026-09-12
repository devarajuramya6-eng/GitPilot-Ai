import { useState } from 'react';
import { 
  ShieldAlert, 
  Cpu, 
  Users, 
  Activity, 
  CheckCircle2, 
  AlertTriangle, 
  Search, 
  Compass, 
  Layers, 
  GitBranch, 
  Bot, 
  Sparkles, 
  Clock, 
  Sliders, 
  ShieldCheck, 
  Zap, 
  Terminal, 
  FolderGit2 
} from 'lucide-react';

// Types
interface IssueItem {
  id: string;
  number: number;
  title: string;
  repo: string;
  type: 'Bug' | 'Feature' | 'Security' | 'Refactor' | 'Performance';
  priority: 'Critical' | 'High' | 'Medium' | 'Low';
  complexity: 'Trivial' | 'Easy' | 'Medium' | 'Hard' | 'Very Hard';
  component: string;
  duplicateProb: number;
  qualityScore: number;
  recommendedDev: {
    name: string;
    score: number;
    workload: 'Light' | 'Optimal' | 'Heavy';
    matchReason: string;
  };
  explanation: string[];
  status: 'triage_pending' | 'auto_approved' | 'action_taken';
  timestamp: string;
}

const SAMPLE_ISSUES: IssueItem[] = [
  {
    id: 'iss-101',
    number: 145,
    title: 'JWT Token rotation fails intermittently under high concurrent requests',
    repo: 'acme/auth-core-service',
    type: 'Security',
    priority: 'Critical',
    complexity: 'Hard',
    component: 'Authentication & Session',
    duplicateProb: 8,
    qualityScore: 92,
    recommendedDev: {
      name: 'Anil K.',
      score: 94,
      workload: 'Optimal',
      matchReason: 'Auth stack authority (24 merged PRs in auth-core), handled 12 similar concurrency issues'
    },
    explanation: [
      'Critical production impact detected from "token rotation fails" in auth pathway',
      'High concurrency stress pattern matches Redis lock contention history',
      'No close duplicates found in active issues (Cosine sim < 0.12)'
    ],
    status: 'triage_pending',
    timestamp: '3 mins ago'
  },
  {
    id: 'iss-102',
    number: 146,
    title: 'Memory leak in streaming response handler when client disconnects early',
    repo: 'acme/gateway-api',
    type: 'Bug',
    priority: 'High',
    complexity: 'Hard',
    component: 'API Gateway / Network',
    duplicateProb: 84,
    qualityScore: 78,
    recommendedDev: {
      name: 'Elena Rostova',
      score: 89,
      workload: 'Optimal',
      matchReason: 'Core maintainer for gateway SSE engine, 98% resolution velocity'
    },
    explanation: [
      'Duplicate candidate detected: 84% semantic similarity with #109 (resolved in v2.4.1)',
      'High severity allocated due to worker container OOM kill risk',
      'Recommended action: Link #109 regression context before manual assignment'
    ],
    status: 'triage_pending',
    timestamp: '18 mins ago'
  },
  {
    id: 'iss-103',
    number: 147,
    title: 'Implement Prometheus exporter for database connection pool statistics',
    repo: 'acme/analytics-pipeline',
    type: 'Feature',
    priority: 'Medium',
    complexity: 'Medium',
    component: 'Observability & Metrics',
    duplicateProb: 4,
    qualityScore: 88,
    recommendedDev: {
      name: 'Marcus Vance',
      score: 86,
      workload: 'Light',
      matchReason: 'Authored metrics pipeline exporter modules, capacity available (1 open task)'
    },
    explanation: [
      'Tagged as Observability expansion based on metric/exporter semantic cluster',
      'Developer workload balanced: Marcus has highest bandwidth among DevOps peers'
    ],
    status: 'auto_approved',
    timestamp: '45 mins ago'
  },
  {
    id: 'iss-104',
    number: 148,
    title: 'CSS layout breaks on ultra-wide viewports in Repository Settings view',
    repo: 'acme/console-ui',
    type: 'Bug',
    priority: 'Low',
    complexity: 'Easy',
    component: 'Frontend / UI',
    duplicateProb: 15,
    qualityScore: 64,
    recommendedDev: {
      name: 'Sarah Chen',
      score: 91,
      workload: 'Heavy',
      matchReason: 'Frontend lead, but currently overloaded (5 in-progress tickets)'
    },
    explanation: [
      'Low priority: Non-blocking visual inconsistency on rare viewport dimensions',
      'Quality score flagged: missing screen captures and browser version details',
      'Workload re-router: recommends reassigning to Junior Frontend engineer'
    ],
    status: 'action_taken',
    timestamp: '2 hours ago'
  }
];

export default function App() {
  const [selectedIssue, setSelectedIssue] = useState<IssueItem>(SAMPLE_ISSUES[0]);
  const [activeTab, setActiveTab] = useState<'control-room' | 'issues' | 'engineers' | 'health' | 'models'>('control-room');
  const [searchQuery, setSearchQuery] = useState('');
  const [issues, setIssues] = useState<IssueItem[]>(SAMPLE_ISSUES);
  const [approvalNotice, setApprovalNotice] = useState<string | null>(null);

  const handleApprove = (issueId: string) => {
    setIssues(prev => prev.map(item => item.id === issueId ? { ...item, status: 'auto_approved' } : item));
    if (selectedIssue.id === issueId) {
      setSelectedIssue(prev => ({ ...prev, status: 'auto_approved' }));
    }
    setApprovalNotice(`Issue #${selectedIssue.number} successfully triaged and assigned to ${selectedIssue.recommendedDev.name} on GitHub!`);
    setTimeout(() => setApprovalNotice(null), 4000);
  };

  return (
    <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9] font-sans antialiased flex flex-col selection:bg-cyan-500 selection:text-white">
      {/* Top Engineering Control Room Header */}
      <header className="border-b border-[#30363d] bg-[#161b22] px-6 py-3 flex items-center justify-between sticky top-0 z-50">
        <div className="flex items-center space-x-3">
          <div className="h-9 w-9 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
            <Cpu className="h-5 w-5 text-white" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <span className="font-bold text-white tracking-wide text-lg">GitPilot AI</span>
              <span className="px-2 py-0.5 rounded text-[11px] font-mono font-medium bg-cyan-950/80 text-cyan-400 border border-cyan-800/60">
                LOCAL ENGINE v1.2
              </span>
            </div>
            <p className="text-xs text-slate-400 font-mono">Turn GitHub Issues Into Intelligent Engineering Decisions</p>
          </div>
        </div>

        {/* Global Navigation Tabs */}
        <nav className="flex items-center space-x-1 bg-[#0d1117] p-1 rounded-lg border border-[#30363d]">
          {[
            { id: 'control-room', label: 'Control Room', icon: Terminal },
            { id: 'issues', label: 'Triage Stream', icon: Layers },
            { id: 'engineers', label: 'Engineer Matching', icon: Users },
            { id: 'health', label: 'Repo Health', icon: Activity },
            { id: 'models', label: 'Local ML Models', icon: Cpu }
          ].map(tab => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as any)}
                className={`flex items-center space-x-2 px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                  isActive 
                    ? 'bg-[#21262d] text-white shadow-sm border border-[#30363d]' 
                    : 'text-slate-400 hover:text-slate-200 hover:bg-[#161b22]'
                }`}
              >
                <Icon className="h-3.5 w-3.5 text-cyan-400" />
                <span>{tab.label}</span>
              </button>
            );
          })}
        </nav>

        {/* Status Indicators */}
        <div className="flex items-center space-x-4 text-xs font-mono">
          <div className="flex items-center space-x-2 bg-[#21262d] px-2.5 py-1 rounded border border-[#30363d]">
            <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse"></span>
            <span className="text-slate-300">Inference: 18ms (Local)</span>
          </div>
          <div className="flex items-center space-x-1.5 text-slate-400">
            <FolderGit2 className="h-4 w-4 text-slate-500" />
            <span>4 Repos Connected</span>
          </div>
        </div>
      </header>

      {/* Alert toast */}
      {approvalNotice && (
        <div className="bg-emerald-950 border-b border-emerald-700/60 px-6 py-2.5 text-emerald-200 text-xs font-mono flex items-center justify-between animate-fadeIn">
          <div className="flex items-center space-x-2">
            <CheckCircle2 className="h-4 w-4 text-emerald-400" />
            <span>{approvalNotice}</span>
          </div>
          <span className="text-emerald-400 text-[10px] uppercase tracking-wider">GitHub API Dispatched</span>
        </div>
      )}

      {/* Main Engineering Workspace */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Column: Repository Intelligence & Issue Queue */}
        <div className="w-[420px] border-r border-[#30363d] bg-[#161b22]/50 flex flex-col flex-shrink-0">
          {/* Quick Metrics Bar */}
          <div className="p-4 border-b border-[#30363d] bg-[#161b22]">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-semibold uppercase tracking-wider text-slate-400 flex items-center gap-1.5">
                <Compass className="h-3.5 w-3.5 text-cyan-400" />
                Active Triage Stream
              </span>
              <span className="text-xs font-mono bg-cyan-950 text-cyan-400 px-2 py-0.5 rounded border border-cyan-800/60">
                {issues.length} Signals
              </span>
            </div>

            <div className="relative">
              <Search className="h-3.5 w-3.5 absolute left-3 top-2.5 text-slate-500" />
              <input
                type="text"
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                placeholder="Search issues, components, errors..."
                className="w-full bg-[#0d1117] border border-[#30363d] rounded-md pl-9 pr-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition-colors font-mono"
              />
            </div>
          </div>

          {/* Issue Stream List */}
          <div className="flex-1 overflow-y-auto divide-y divide-[#21262d]">
            {issues
              .filter(i => i.title.toLowerCase().includes(searchQuery.toLowerCase()) || i.component.toLowerCase().includes(searchQuery.toLowerCase()))
              .map(issue => {
                const isSelected = selectedIssue.id === issue.id;
                return (
                  <div
                    key={issue.id}
                    onClick={() => setSelectedIssue(issue)}
                    className={`p-4 cursor-pointer transition-all ${
                      isSelected
                        ? 'bg-[#1c2128] border-l-4 border-cyan-400'
                        : 'hover:bg-[#161b22] border-l-4 border-transparent'
                    }`}
                  >
                    <div className="flex items-center justify-between mb-1.5">
                      <div className="flex items-center space-x-2">
                        <span className="text-xs font-mono text-cyan-400 font-medium">#{issue.number}</span>
                        <span className="text-[11px] font-mono text-slate-400 truncate max-w-[140px]">{issue.repo.split('/')[1]}</span>
                      </div>
                      <span className={`text-[10px] px-2 py-0.5 rounded font-mono font-medium ${
                        issue.priority === 'Critical' ? 'bg-red-950 text-red-400 border border-red-800/60' :
                        issue.priority === 'High' ? 'bg-amber-950 text-amber-400 border border-amber-800/60' :
                        'bg-blue-950 text-blue-400 border border-blue-800/60'
                      }`}>
                        {issue.priority}
                      </span>
                    </div>

                    <h4 className="text-xs font-medium text-slate-200 line-clamp-2 leading-relaxed mb-2.5">
                      {issue.title}
                    </h4>

                    <div className="flex items-center justify-between text-[11px] text-slate-400">
                      <span className="inline-flex items-center gap-1 text-slate-300">
                        <Layers className="h-3 w-3 text-slate-500" />
                        {issue.component}
                      </span>
                      <div className="flex items-center gap-2">
                        {issue.duplicateProb > 50 && (
                          <span className="text-[10px] font-mono text-amber-400 bg-amber-950/80 px-1.5 py-0.5 rounded border border-amber-800/40">
                            {issue.duplicateProb}% Dup
                          </span>
                        )}
                        <span className="font-mono text-[10px] text-slate-500">{issue.timestamp}</span>
                      </div>
                    </div>
                  </div>
                );
              })}
          </div>

          {/* Quick Metrics Footer */}
          <div className="p-3 border-t border-[#30363d] bg-[#161b22] flex items-center justify-between text-[11px] font-mono text-slate-400">
            <div className="flex items-center gap-2">
              <Activity className="h-3.5 w-3.5 text-cyan-400" />
              <span>Queue Status: Normal</span>
            </div>
            <span className="text-slate-500">Auto-triage: Active</span>
          </div>
        </div>

        {/* Right Column: Deep Explainable AI Triage & Engineering Decision Center */}
        <div className="flex-1 bg-[#0d1117] flex flex-col overflow-y-auto">
          {/* Header Panel for Selected Issue */}
          <div className="p-6 border-b border-[#30363d] bg-[#161b22]/40">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center space-x-3">
                <span className="px-2.5 py-1 rounded bg-[#21262d] text-cyan-400 font-mono font-bold text-sm border border-[#30363d]">
                  ISSUE #{selectedIssue.number}
                </span>
                <span className="text-xs text-slate-400 font-mono">{selectedIssue.repo}</span>
                <span className="text-xs text-slate-500 font-mono">• Reported {selectedIssue.timestamp}</span>
              </div>
              <div className="flex items-center space-x-2">
                <button 
                  onClick={() => handleApprove(selectedIssue.id)}
                  disabled={selectedIssue.status === 'auto_approved'}
                  className={`px-3 py-1.5 rounded text-xs font-semibold flex items-center space-x-1.5 transition-all ${
                    selectedIssue.status === 'auto_approved'
                      ? 'bg-emerald-950 text-emerald-400 border border-emerald-800 cursor-default'
                      : 'bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white shadow-md shadow-cyan-600/20'
                  }`}
                >
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  <span>{selectedIssue.status === 'auto_approved' ? 'Assigned & Labeled' : 'Apply AI Decisions to GitHub'}</span>
                </button>
              </div>
            </div>

            <h1 className="text-lg font-semibold text-white mb-4 leading-snug">
              {selectedIssue.title}
            </h1>

            {/* Quick Signal Badges */}
            <div className="grid grid-cols-5 gap-3 pt-2 font-mono">
              <div className="bg-[#161b22] p-2.5 rounded border border-[#30363d]">
                <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Issue Class</div>
                <div className="text-xs font-semibold text-cyan-400 flex items-center gap-1.5">
                  <Bot className="h-3.5 w-3.5" />
                  {selectedIssue.type}
                </div>
              </div>
              <div className="bg-[#161b22] p-2.5 rounded border border-[#30363d]">
                <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Priority Level</div>
                <div className="text-xs font-semibold text-amber-400 flex items-center gap-1.5">
                  <AlertTriangle className="h-3.5 w-3.5" />
                  {selectedIssue.priority}
                </div>
              </div>
              <div className="bg-[#161b22] p-2.5 rounded border border-[#30363d]">
                <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Complexity</div>
                <div className="text-xs font-semibold text-purple-400 flex items-center gap-1.5">
                  <Sliders className="h-3.5 w-3.5" />
                  {selectedIssue.complexity}
                </div>
              </div>
              <div className="bg-[#161b22] p-2.5 rounded border border-[#30363d]">
                <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Duplicate Risk</div>
                <div className={`text-xs font-semibold flex items-center gap-1.5 ${selectedIssue.duplicateProb > 40 ? 'text-amber-400' : 'text-emerald-400'}`}>
                  <GitBranch className="h-3.5 w-3.5" />
                  {selectedIssue.duplicateProb}%
                </div>
              </div>
              <div className="bg-[#161b22] p-2.5 rounded border border-[#30363d]">
                <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Issue Quality</div>
                <div className="text-xs font-semibold text-cyan-400 flex items-center gap-1.5">
                  <Sparkles className="h-3.5 w-3.5" />
                  {selectedIssue.qualityScore}/100
                </div>
              </div>
            </div>
          </div>

          {/* Explainable Decision Core */}
          <div className="p-6 grid grid-cols-2 gap-6">
            {/* Engineer Recommendation Engine */}
            <div className="bg-[#161b22] p-5 rounded-lg border border-[#30363d]">
              <div className="flex items-center justify-between mb-4 border-b border-[#30363d] pb-3">
                <span className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center gap-2 font-mono">
                  <Users className="h-4 w-4 text-cyan-400" />
                  Intelligent Developer Match
                </span>
                <span className="text-xs font-mono text-emerald-400 bg-emerald-950 px-2 py-0.5 rounded border border-emerald-800">
                  {selectedIssue.recommendedDev.score}% Confidence
                </span>
              </div>

              <div className="flex items-start space-x-4 mb-4">
                <div className="h-11 w-11 rounded-full bg-gradient-to-tr from-cyan-600 to-emerald-600 flex items-center justify-center font-bold text-white shadow-md">
                  {selectedIssue.recommendedDev.name.substring(0, 2).toUpperCase()}
                </div>
                <div>
                  <h3 className="font-semibold text-white text-sm">{selectedIssue.recommendedDev.name}</h3>
                  <div className="flex items-center space-x-2 text-xs font-mono text-slate-400 mt-0.5">
                    <span>Workload: <strong className="text-emerald-400">{selectedIssue.recommendedDev.workload}</strong></span>
                    <span>•</span>
                    <span>Component Specialist</span>
                  </div>
                </div>
              </div>

              <div className="bg-[#0d1117] p-3 rounded border border-[#30363d] mb-4">
                <div className="text-[11px] font-mono text-slate-400 uppercase tracking-wide mb-1">Matching Rationale</div>
                <p className="text-xs text-slate-300 leading-relaxed font-sans">
                  {selectedIssue.recommendedDev.matchReason}
                </p>
              </div>

              {/* Workload Balancer Guardrail */}
              <div className="text-xs text-slate-400 flex items-center justify-between border-t border-[#30363d] pt-3 font-mono">
                <span>Workload Balancer Guardrail</span>
                <span className="text-emerald-400 flex items-center gap-1">
                  <ShieldCheck className="h-3.5 w-3.5" />
                  Within Safe Quota
                </span>
              </div>
            </div>

            {/* Explainable AI Rationale */}
            <div className="bg-[#161b22] p-5 rounded-lg border border-[#30363d] flex flex-col justify-between">
              <div>
                <div className="flex items-center justify-between mb-4 border-b border-[#30363d] pb-3">
                  <span className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center gap-2 font-mono">
                    <Zap className="h-4 w-4 text-amber-400" />
                    Explainable AI Rationale
                  </span>
                  <span className="text-xs font-mono text-slate-400">Deterministic Features</span>
                </div>

                <ul className="space-y-2.5">
                  {selectedIssue.explanation.map((item, idx) => (
                    <li key={idx} className="flex items-start space-x-2.5 text-xs text-slate-300 leading-relaxed">
                      <CheckCircle2 className="h-4 w-4 text-cyan-400 flex-shrink-0 mt-0.5" />
                      <span>{item}</span>
                    </li>
                  ))}
                </ul>
              </div>

              <div className="mt-4 pt-4 border-t border-[#30363d] bg-[#0d1117]/60 p-3 rounded text-[11px] font-mono text-slate-400">
                <span className="text-slate-300 font-semibold block mb-0.5">Local ML Pipeline Verification:</span>
                Trained on 1,420 historical repository issues. Zero external LLM calls invoked.
              </div>
            </div>
          </div>

          {/* Repository Memory Engine & Engineering Health Signals */}
          <div className="p-6 pt-0 grid grid-cols-3 gap-6">
            <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
              <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2">
                <Clock className="h-3.5 w-3.5 text-cyan-400" />
                Repository Memory
              </div>
              <div className="text-xs text-slate-300 space-y-2">
                <p>• Historical MTTR for Authentication: <strong>4.2 hours</strong></p>
                <p>• 91% resolved without regression</p>
                <p>• Top historical resolver: <strong>Anil K.</strong></p>
              </div>
            </div>

            <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
              <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2">
                <Activity className="h-3.5 w-3.5 text-emerald-400" />
                Engineering Health
              </div>
              <div className="text-xs text-slate-300 space-y-2">
                <p>• Repository Health Score: <strong className="text-emerald-400">84/100</strong></p>
                <p>• Duplicate prevention rate: <strong>94%</strong></p>
                <p>• Avg triage response time: <strong>1.4 mins</strong></p>
              </div>
            </div>

            <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
              <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2">
                <ShieldAlert className="h-3.5 w-3.5 text-purple-400" />
                Human-in-the-Loop Mode
              </div>
              <div className="text-xs text-slate-300 space-y-2">
                <p>• Current Threshold: <strong>85%</strong></p>
                <p>• Status: <strong>Maintainer Approval</strong></p>
                <p>• Audit Log: <strong>Recorded</strong></p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
