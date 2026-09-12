import { useState } from 'react';
import { Link } from 'react-router-dom';
import {
  Cpu, Layers, Users, Activity, GitBranch, AlertTriangle,
  CheckCircle2, Clock, Bot, Sparkles, Sliders, ShieldCheck,
  Zap, Terminal, FolderGit2, Search, Bell, Settings, LogOut,
  BarChart3, TrendingUp
} from 'lucide-react';
import { useAuthStore } from '../store/authStore';
import toast from 'react-hot-toast';

const SAMPLE_ISSUES = [
  {
    id: 'iss-101', number: 145,
    title: 'JWT Token rotation fails intermittently under high concurrent requests',
    repo: 'auth-core-service', type: 'Security', priority: 'Critical',
    complexity: 'Hard', component: 'Authentication & Session',
    duplicateProb: 8, qualityScore: 92,
    recommendedDev: { name: 'Anil K.', score: 94, workload: 'Optimal', matchReason: 'Auth stack authority (24 merged PRs in auth-core), handled 12 similar concurrency issues' },
    explanation: ['Critical production impact detected from "token rotation fails" in auth pathway', 'High concurrency stress pattern matches Redis lock contention history', 'No close duplicates found in active issues (Cosine sim < 0.12)'],
    status: 'triage_pending', timestamp: '3 mins ago'
  },
  {
    id: 'iss-102', number: 146,
    title: 'Memory leak in streaming response handler when client disconnects early',
    repo: 'gateway-api', type: 'Bug', priority: 'High',
    complexity: 'Hard', component: 'API Gateway / Network',
    duplicateProb: 84, qualityScore: 78,
    recommendedDev: { name: 'Elena R.', score: 89, workload: 'Optimal', matchReason: 'Core maintainer for gateway SSE engine, 98% resolution velocity' },
    explanation: ['Duplicate candidate detected: 84% semantic similarity with #109 (resolved in v2.4.1)', 'High severity allocated due to worker container OOM kill risk'],
    status: 'triage_pending', timestamp: '18 mins ago'
  },
  {
    id: 'iss-103', number: 147,
    title: 'Implement Prometheus exporter for database connection pool statistics',
    repo: 'analytics-pipeline', type: 'Feature', priority: 'Medium',
    complexity: 'Medium', component: 'Observability & Metrics',
    duplicateProb: 4, qualityScore: 88,
    recommendedDev: { name: 'Marcus V.', score: 86, workload: 'Light', matchReason: 'Authored metrics pipeline exporter modules, capacity available (1 open task)' },
    explanation: ['Tagged as Observability expansion based on metric/exporter semantic cluster', 'Developer workload balanced: Marcus has highest bandwidth among DevOps peers'],
    status: 'auto_approved', timestamp: '45 mins ago'
  },
  {
    id: 'iss-104', number: 148,
    title: 'CSS layout breaks on ultra-wide viewports in Repository Settings view',
    repo: 'console-ui', type: 'Bug', priority: 'Low',
    complexity: 'Easy', component: 'Frontend / UI',
    duplicateProb: 15, qualityScore: 64,
    recommendedDev: { name: 'Sarah C.', score: 91, workload: 'Heavy', matchReason: 'Frontend lead, but currently overloaded (5 in-progress tickets)' },
    explanation: ['Low priority: Non-blocking visual inconsistency on rare viewport dimensions', 'Quality score flagged: missing screen captures and browser version details'],
    status: 'action_taken', timestamp: '2 hours ago'
  }
];

const HEALTH_METRICS = [
  { label: 'Issue Resolution', score: 82, trend: '+3' },
  { label: 'Duplicate Prevention', score: 94, trend: '+5' },
  { label: 'Response Time', score: 88, trend: '0' },
  { label: 'Developer Load Balance', score: 69, trend: '-2' },
  { label: 'Security Coverage', score: 91, trend: '+1' },
  { label: 'Overall Health', score: 84, trend: '+2' },
];

type Issue = typeof SAMPLE_ISSUES[0];

export default function DashboardPage() {
  const { user, logout } = useAuthStore();
  const [selected, setSelected] = useState<Issue>(SAMPLE_ISSUES[0]);
  const [issues, setIssues] = useState(SAMPLE_ISSUES);
  const [search, setSearch] = useState('');

  const handleApprove = (id: string) => {
    setIssues(prev => prev.map(i => i.id === id ? { ...i, status: 'auto_approved' } : i));
    if (selected.id === id) setSelected(prev => ({ ...prev, status: 'auto_approved' }));
    toast.success(`Issue #${selected.number} triaged & assigned to ${selected.recommendedDev.name} on GitHub`);
  };

  const priorityBadge = (p: string) =>
    p === 'Critical' ? 'bg-red-950 text-red-400 border-red-800/60'
    : p === 'High' ? 'bg-amber-950 text-amber-400 border-amber-800/60'
    : p === 'Medium' ? 'bg-blue-950 text-blue-400 border-blue-800/60'
    : 'bg-slate-900 text-slate-400 border-slate-700';

  return (
    <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9] font-sans flex flex-col">

      {/* ── Header ── */}
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

        <nav className="flex items-center space-x-1 bg-[#0d1117] px-1 py-1 rounded-lg border border-[#30363d]">
          {[
            { label: 'Control Room', icon: Terminal, active: true },
            { label: 'Issues', icon: Layers, active: false },
            { label: 'Engineers', icon: Users, active: false },
            { label: 'Health', icon: Activity, active: false },
            { label: 'ML Models', icon: Cpu, active: false },
          ].map(tab => {
            const Icon = tab.icon;
            return (
              <button key={tab.label} className={`flex items-center space-x-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all ${tab.active ? 'bg-[#21262d] text-white border border-[#30363d]' : 'text-slate-400 hover:text-slate-200'}`}>
                <Icon className="h-3.5 w-3.5 text-cyan-400" />
                <span>{tab.label}</span>
              </button>
            );
          })}
        </nav>

        <div className="flex items-center space-x-3">
          <div className="flex items-center space-x-2 bg-[#21262d] px-2.5 py-1 rounded border border-[#30363d] text-xs font-mono">
            <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
            <span className="text-slate-300">Inference: 18ms</span>
          </div>
          <button className="text-slate-400 hover:text-white transition-colors p-1.5"><Bell className="h-4 w-4" /></button>
          <Link to="/connect" className="text-slate-400 hover:text-white transition-colors p-1.5"><FolderGit2 className="h-4 w-4" /></Link>
          <button className="text-slate-400 hover:text-white transition-colors p-1.5"><Settings className="h-4 w-4" /></button>
          <button onClick={() => { logout(); toast('Signed out'); }} className="text-slate-400 hover:text-rose-400 transition-colors p-1.5"><LogOut className="h-4 w-4" /></button>
          <div className="h-7 w-7 rounded-full bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-xs font-bold text-white">
            {user?.email?.substring(0, 2).toUpperCase() || 'U'}
          </div>
        </div>
      </header>

      {/* ── Body ── */}
      <div className="flex-1 flex overflow-hidden">

        {/* ── Issue Queue ── */}
        <div className="w-[380px] border-r border-[#30363d] bg-[#161b22]/30 flex flex-col flex-shrink-0">
          <div className="p-4 border-b border-[#30363d]">
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-semibold uppercase tracking-wider text-slate-400 flex items-center gap-1.5">
                <Layers className="h-3.5 w-3.5 text-cyan-400" />
                Triage Stream
              </span>
              <span className="text-xs font-mono bg-cyan-950 text-cyan-400 px-2 py-0.5 rounded border border-cyan-800/60">{issues.length} signals</span>
            </div>
            <div className="relative">
              <Search className="h-3.5 w-3.5 absolute left-3 top-2.5 text-slate-500" />
              <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search issues..." className="w-full bg-[#0d1117] border border-[#30363d] rounded-md pl-9 pr-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition-colors font-mono" />
            </div>
          </div>

          <div className="flex-1 overflow-y-auto divide-y divide-[#21262d]">
            {issues.filter(i => i.title.toLowerCase().includes(search.toLowerCase()) || i.component.toLowerCase().includes(search.toLowerCase())).map(issue => (
              <div key={issue.id} onClick={() => setSelected(issue)} className={`p-4 cursor-pointer transition-all ${selected.id === issue.id ? 'bg-[#1c2128] border-l-4 border-cyan-400' : 'hover:bg-[#161b22] border-l-4 border-transparent'}`}>
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-xs font-mono text-cyan-400 font-medium">#{issue.number}</span>
                  <span className={`text-[10px] px-2 py-0.5 rounded font-mono font-medium border ${priorityBadge(issue.priority)}`}>{issue.priority}</span>
                </div>
                <h4 className="text-xs font-medium text-slate-200 line-clamp-2 leading-relaxed mb-2">{issue.title}</h4>
                <div className="flex items-center justify-between text-[11px] text-slate-400">
                  <span className="truncate max-w-[180px]">{issue.component}</span>
                  <div className="flex items-center gap-2">
                    {issue.duplicateProb > 50 && <span className="text-[10px] font-mono text-amber-400 bg-amber-950/80 px-1.5 py-0.5 rounded border border-amber-800/40">{issue.duplicateProb}% dup</span>}
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

        {/* ── Triage Detail ── */}
        <div className="flex-1 flex flex-col overflow-y-auto">

          {/* Issue Header */}
          <div className="p-5 border-b border-[#30363d] bg-[#161b22]/30">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center space-x-3">
                <span className="px-2.5 py-1 rounded bg-[#21262d] text-cyan-400 font-mono font-bold text-sm border border-[#30363d]">ISSUE #{selected.number}</span>
                <span className="text-xs text-slate-400 font-mono">{selected.repo}</span>
              </div>
              <button
                onClick={() => handleApprove(selected.id)}
                disabled={selected.status === 'auto_approved'}
                className={`px-3 py-1.5 rounded text-xs font-semibold flex items-center space-x-1.5 transition-all ${selected.status === 'auto_approved' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800 cursor-default' : 'bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white shadow-md shadow-cyan-600/20'}`}
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
                    <div className={`text-xs font-semibold ${sig.color} flex items-center gap-1.5`}><Icon className="h-3.5 w-3.5" />{sig.value}</div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Detail Panels */}
          <div className="p-5 grid grid-cols-2 gap-5">

            {/* Developer Match */}
            <div className="bg-[#161b22] p-5 rounded-lg border border-[#30363d]">
              <div className="flex items-center justify-between mb-4 border-b border-[#30363d] pb-3">
                <span className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center gap-2 font-mono"><Users className="h-4 w-4 text-cyan-400" />Developer Match</span>
                <span className="text-xs font-mono text-emerald-400 bg-emerald-950 px-2 py-0.5 rounded border border-emerald-800">{selected.recommendedDev.score}% Confidence</span>
              </div>
              <div className="flex items-start space-x-3 mb-4">
                <div className="h-10 w-10 rounded-full bg-gradient-to-tr from-cyan-600 to-emerald-600 flex items-center justify-center font-bold text-white text-sm shadow-md">
                  {selected.recommendedDev.name.substring(0, 2).toUpperCase()}
                </div>
                <div>
                  <h3 className="font-semibold text-white text-sm">{selected.recommendedDev.name}</h3>
                  <div className="text-xs font-mono text-slate-400">Workload: <strong className="text-emerald-400">{selected.recommendedDev.workload}</strong></div>
                </div>
              </div>
              <div className="bg-[#0d1117] p-3 rounded border border-[#30363d]">
                <div className="text-[11px] font-mono text-slate-400 uppercase tracking-wide mb-1">Rationale</div>
                <p className="text-xs text-slate-300 leading-relaxed">{selected.recommendedDev.matchReason}</p>
              </div>
              <div className="text-xs text-slate-400 flex items-center justify-between border-t border-[#30363d] pt-3 mt-3 font-mono">
                <span>Workload Balancer</span>
                <span className="text-emerald-400 flex items-center gap-1"><ShieldCheck className="h-3.5 w-3.5" />Safe Quota</span>
              </div>
            </div>

            {/* Explainable AI */}
            <div className="bg-[#161b22] p-5 rounded-lg border border-[#30363d] flex flex-col">
              <div className="flex items-center justify-between mb-4 border-b border-[#30363d] pb-3">
                <span className="text-xs font-bold uppercase tracking-wider text-slate-300 flex items-center gap-2 font-mono"><Zap className="h-4 w-4 text-amber-400" />AI Rationale</span>
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

          {/* Health + Memory + Mode */}
          <div className="px-5 pb-5 grid grid-cols-3 gap-4">
            <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
              <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2"><BarChart3 className="h-3.5 w-3.5 text-cyan-400" />Repo Health</div>
              <div className="space-y-2">
                {HEALTH_METRICS.slice(0, 4).map(m => (
                  <div key={m.label} className="flex items-center justify-between text-xs">
                    <span className="text-slate-400 font-mono">{m.label}</span>
                    <div className="flex items-center gap-2">
                      <div className="w-16 bg-[#0d1117] rounded-full h-1 overflow-hidden">
                        <div className="h-full bg-cyan-500 rounded-full" style={{ width: `${m.score}%` }} />
                      </div>
                      <span className="text-slate-300 font-mono w-7 text-right">{m.score}</span>
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-3 pt-3 border-t border-[#30363d] flex items-center justify-between text-xs font-mono">
                <span className="text-slate-400">Overall</span>
                <span className="text-emerald-400 font-bold">84/100</span>
              </div>
            </div>

            <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
              <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2"><Clock className="h-3.5 w-3.5 text-emerald-400" />Repository Memory</div>
              <div className="text-xs text-slate-300 space-y-1.5 font-mono">
                <p>Auth MTTR: <strong className="text-white">4.2 hours</strong></p>
                <p>Resolution rate: <strong className="text-emerald-400">91%</strong></p>
                <p>Top resolver: <strong className="text-white">Anil K.</strong></p>
                <p>Issues analyzed: <strong className="text-white">1,420</strong></p>
                <p>Memory depth: <strong className="text-white">18 months</strong></p>
              </div>
            </div>

            <div className="bg-[#161b22] p-4 rounded-lg border border-[#30363d]">
              <div className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3 font-mono flex items-center gap-2"><TrendingUp className="h-3.5 w-3.5 text-purple-400" />Triage Metrics</div>
              <div className="text-xs text-slate-300 space-y-1.5 font-mono">
                <p>Avg inference: <strong className="text-white">18ms</strong></p>
                <p>Auto-approved today: <strong className="text-emerald-400">12</strong></p>
                <p>Pending approval: <strong className="text-amber-400">3</strong></p>
                <p>Duplicates caught: <strong className="text-white">5</strong></p>
                <p>Classification acc: <strong className="text-cyan-400">91%</strong></p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
