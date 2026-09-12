import { Link } from 'react-router-dom';
import {
  Cpu, GitBranch, Zap, ShieldCheck, Users, ArrowRight,
  CheckCircle2, Terminal, Activity, Layers, Bot, Star
} from 'lucide-react';

const FEATURES = [
  {
    icon: Bot,
    color: 'cyan',
    title: 'Intelligent Issue Classification',
    description: 'TF-IDF + SVM classifiers categorize every new issue into Bug, Security, Feature, Performance and 8 other types within milliseconds — all running locally on your infrastructure.'
  },
  {
    icon: Users,
    color: 'emerald',
    title: 'Developer Matching Engine',
    description: 'Scores every developer against an issue using historical commit data, component ownership, workload, and resolution velocity. Shows top 3 candidates with explainable rationale.'
  },
  {
    icon: GitBranch,
    color: 'amber',
    title: 'Duplicate Detection',
    description: 'Cosine similarity on TF-IDF embeddings catches near-duplicate issues before they pile up. Surfaces candidates ranked by similarity percentage, requiring human confirmation before merge.'
  },
  {
    icon: Zap,
    color: 'purple',
    title: 'Priority Prediction',
    description: 'Random Forest classifiers analyze keywords, component context, crash indicators, and historical patterns to predict Critical/High/Medium/Low priority with confidence scores.'
  },
  {
    icon: Activity,
    color: 'rose',
    title: 'Engineering Health Score',
    description: 'A composite 0–100 score tracking issue resolution time, duplicate rate, response velocity, developer load balance, documentation quality, and security hygiene per repository.'
  },
  {
    icon: ShieldCheck,
    color: 'teal',
    title: 'Human-in-the-Loop Approval',
    description: 'Three-tier decision mode: Auto (>85% confidence), Approval (60–85%), and Manual Review (<60%). Thresholds are fully configurable per repository.'
  },
];

const STATS = [
  { label: 'Avg. Triage Time', value: '< 2s', sub: 'Local inference' },
  { label: 'Classification Accuracy', value: '91%', sub: 'On held-out test set' },
  { label: 'Developer Match Top-3', value: '96%', sub: 'Historical validation' },
  { label: 'Duplicate Precision', value: '88%', sub: 'At 0.75 threshold' },
];

const WORKFLOW_STEPS = [
  { step: '01', title: 'GitHub Issue Arrives', desc: 'Webhook fires instantly' },
  { step: '02', title: 'Text Preprocessing', desc: 'Normalize, tokenize, extract signals' },
  { step: '03', title: 'Local ML Pipeline', desc: 'TF-IDF → Classifier Ensemble' },
  { step: '04', title: 'Explainable Result', desc: 'Type, priority, component, complexity' },
  { step: '05', title: 'Developer Scoring', desc: 'Skill + workload + history match' },
  { step: '06', title: 'Human Decision', desc: 'Approve, adjust, or auto-apply' },
];

const colorMap: Record<string, string> = {
  cyan: 'text-cyan-400 bg-cyan-950 border-cyan-800/60',
  emerald: 'text-emerald-400 bg-emerald-950 border-emerald-800/60',
  amber: 'text-amber-400 bg-amber-950 border-amber-800/60',
  purple: 'text-purple-400 bg-purple-950 border-purple-800/60',
  rose: 'text-rose-400 bg-rose-950 border-rose-800/60',
  teal: 'text-teal-400 bg-teal-950 border-teal-800/60',
};

export default function HomePage() {
  return (
    <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9] font-sans">

      {/* ── Navigation ── */}
      <nav className="border-b border-[#21262d] bg-[#0d1117]/90 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
              <Cpu className="h-4 w-4 text-white" />
            </div>
            <span className="font-bold text-white text-base tracking-wide">GitPilot AI</span>
            <span className="hidden sm:inline px-2 py-0.5 rounded text-[10px] font-mono font-medium bg-cyan-950 text-cyan-400 border border-cyan-800/60">
              LOCAL ENGINE
            </span>
          </div>

          <div className="hidden md:flex items-center space-x-6 text-sm text-slate-400">
            <a href="#features" className="hover:text-white transition-colors">Features</a>
            <a href="#workflow" className="hover:text-white transition-colors">How It Works</a>
            <a href="#metrics" className="hover:text-white transition-colors">Accuracy</a>
          </div>

          <div className="flex items-center space-x-3">
            <Link to="/login" className="text-sm text-slate-300 hover:text-white transition-colors px-3 py-1.5">
              Sign In
            </Link>
            <Link
              to="/signup"
              className="px-4 py-1.5 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white text-sm font-semibold rounded-lg transition-all shadow-md shadow-cyan-700/30"
            >
              Start Free
            </Link>
          </div>
        </div>
      </nav>

      {/* ── Hero ── */}
      <section className="relative overflow-hidden">
        {/* Grid background */}
        <div
          className="absolute inset-0 opacity-[0.04]"
          style={{
            backgroundImage:
              'linear-gradient(#c9d1d9 1px, transparent 1px), linear-gradient(to right, #c9d1d9 1px, transparent 1px)',
            backgroundSize: '48px 48px',
          }}
        />
        {/* Glow orbs */}
        <div className="absolute top-16 left-1/2 -translate-x-1/2 h-[400px] w-[800px] bg-cyan-600/10 rounded-full blur-3xl pointer-events-none" />
        <div className="absolute top-40 right-0 h-[300px] w-[400px] bg-blue-600/8 rounded-full blur-3xl pointer-events-none" />

        <div className="relative max-w-6xl mx-auto px-6 pt-24 pb-20 text-center">

          {/* Badge */}
          <div className="inline-flex items-center space-x-2 px-3 py-1 rounded-full bg-[#161b22] border border-[#30363d] text-xs font-mono text-slate-400 mb-8">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
            <span>Local ML Engine — Zero external AI APIs — Fully private</span>
          </div>

          {/* Headline */}
          <h1 className="text-5xl md:text-6xl font-bold text-white leading-tight mb-6 tracking-tight">
            Turn GitHub Issues Into<br />
            <span className="text-transparent bg-clip-text bg-gradient-to-r from-cyan-400 to-blue-500">
              Intelligent Engineering Decisions
            </span>
          </h1>

          <p className="text-xl text-slate-400 max-w-3xl mx-auto leading-relaxed mb-10">
            GitPilot AI analyzes GitHub issues using locally-running machine learning models,
            automatically triages priority and complexity, detects duplicates, matches the right
            developer, and explains every decision — with no data sent to external AI services.
          </p>

          {/* CTA Buttons */}
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4 mb-16">
            <Link
              to="/signup"
              className="group flex items-center space-x-2 px-6 py-3 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white font-semibold rounded-xl transition-all shadow-xl shadow-cyan-700/30 text-sm"
            >
              <span>Launch Engineering Control Room</span>
              <ArrowRight className="h-4 w-4 group-hover:translate-x-1 transition-transform" />
            </Link>
            <Link
              to="/login"
              className="flex items-center space-x-2 px-6 py-3 bg-[#161b22] hover:bg-[#21262d] text-slate-300 font-semibold rounded-xl border border-[#30363d] transition-all text-sm"
            >
              <Terminal className="h-4 w-4 text-cyan-400" />
              <span>View Live Demo</span>
            </Link>
          </div>

          {/* Terminal-style Preview */}
          <div className="max-w-3xl mx-auto bg-[#161b22] border border-[#30363d] rounded-2xl overflow-hidden shadow-2xl shadow-black/60 text-left">
            {/* Title bar */}
            <div className="flex items-center px-4 py-2.5 bg-[#21262d] border-b border-[#30363d]">
              <div className="flex space-x-1.5">
                <div className="h-3 w-3 rounded-full bg-rose-500/60"></div>
                <div className="h-3 w-3 rounded-full bg-amber-500/60"></div>
                <div className="h-3 w-3 rounded-full bg-emerald-500/60"></div>
              </div>
              <span className="ml-4 text-xs font-mono text-slate-400">gitpilot — local_ml_engine.py</span>
            </div>
            {/* Log lines */}
            <div className="p-5 font-mono text-xs space-y-2 text-slate-300">
              <p><span className="text-slate-500">10:31:02</span>  <span className="text-cyan-400">[WEBHOOK]</span> Issue #145 received — "Login crashes on invalid credentials"</p>
              <p><span className="text-slate-500">10:31:02</span>  <span className="text-amber-400">[PREPROC]</span> Tokenized 42 tokens, 6 technical signals extracted</p>
              <p><span className="text-slate-500">10:31:02</span>  <span className="text-purple-400">[TF-IDF]</span>  Feature vector: 4096 dimensions → Linear SVM inference</p>
              <p><span className="text-slate-500">10:31:02</span>  <span className="text-cyan-400">[CLASSIFY]</span> <span className="text-white font-semibold">BUG</span> — confidence 94.2% | Priority: <span className="text-red-400 font-semibold">HIGH</span> — 91.1%</p>
              <p><span className="text-slate-500">10:31:02</span>  <span className="text-emerald-400">[DEDUP]</span>   Scanned 412 open issues. Top match: #102 @ 84.3% similarity</p>
              <p><span className="text-slate-500">10:31:03</span>  <span className="text-amber-400">[DEV-MATCH]</span> Ranked 12 engineers. Recommendation: <span className="text-white font-semibold">Anil K.</span> (score: 94%)</p>
              <p><span className="text-slate-500">10:31:03</span>  <span className="text-cyan-400">[DECISION]</span> Confidence 91% → <span className="text-amber-400">Maintainer Approval Required</span></p>
              <p><span className="text-slate-500">10:31:03</span>  <span className="text-emerald-400">[DONE]</span>    Total inference time: <span className="text-white">18ms</span> — zero external API calls</p>
            </div>
          </div>
        </div>
      </section>

      {/* ── Stats ── */}
      <section id="metrics" className="border-y border-[#21262d] bg-[#0d1117]/80">
        <div className="max-w-6xl mx-auto px-6 py-12 grid grid-cols-2 md:grid-cols-4 gap-8">
          {STATS.map((s) => (
            <div key={s.label} className="text-center">
              <div className="text-3xl font-bold text-white mb-1">{s.value}</div>
              <div className="text-sm font-semibold text-slate-200 mb-0.5">{s.label}</div>
              <div className="text-xs text-slate-500 font-mono">{s.sub}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ── Features ── */}
      <section id="features" className="py-24">
        <div className="max-w-6xl mx-auto px-6">
          <div className="text-center mb-16">
            <div className="inline-flex items-center space-x-2 px-3 py-1 rounded-full bg-[#161b22] border border-[#30363d] text-xs font-mono text-slate-400 mb-4">
              <Layers className="h-3.5 w-3.5 text-cyan-400" />
              <span>Core Intelligence Modules</span>
            </div>
            <h2 className="text-3xl font-bold text-white mb-4">Everything a team needs to triage smarter</h2>
            <p className="text-slate-400 max-w-2xl mx-auto">
              Each module is a deterministic, auditable ML component — no black-box LLM, no external dependency, no data leaving your network.
            </p>
          </div>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-5">
            {FEATURES.map((f) => {
              const Icon = f.icon;
              const cls = colorMap[f.color];
              return (
                <div
                  key={f.title}
                  className="bg-[#161b22] border border-[#30363d] rounded-xl p-5 hover:border-[#484f58] transition-all group"
                >
                  <div className={`inline-flex items-center justify-center h-9 w-9 rounded-lg border ${cls} mb-4`}>
                    <Icon className={`h-5 w-5 ${cls.split(' ')[0]}`} />
                  </div>
                  <h3 className="font-semibold text-white mb-2 group-hover:text-cyan-300 transition-colors text-sm">{f.title}</h3>
                  <p className="text-xs text-slate-400 leading-relaxed">{f.description}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* ── Workflow ── */}
      <section id="workflow" className="py-20 bg-[#161b22]/50 border-y border-[#21262d]">
        <div className="max-w-6xl mx-auto px-6">
          <div className="text-center mb-14">
            <h2 className="text-3xl font-bold text-white mb-3">How the triage pipeline works</h2>
            <p className="text-slate-400 text-sm">From raw GitHub event to explainable engineering decision in under 2 seconds</p>
          </div>

          <div className="grid md:grid-cols-3 lg:grid-cols-6 gap-4">
            {WORKFLOW_STEPS.map((s, i) => (
              <div key={s.step} className="relative flex flex-col items-center text-center">
                {i < WORKFLOW_STEPS.length - 1 && (
                  <div className="hidden lg:block absolute top-5 left-[58%] w-full h-px bg-gradient-to-r from-cyan-500/40 to-transparent" />
                )}
                <div className="h-10 w-10 rounded-full bg-[#21262d] border border-cyan-500/40 flex items-center justify-center font-mono text-xs font-bold text-cyan-400 mb-3 z-10">
                  {s.step}
                </div>
                <div className="text-sm font-semibold text-white mb-1">{s.title}</div>
                <div className="text-xs text-slate-500">{s.desc}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── CTA Banner ── */}
      <section className="py-20">
        <div className="max-w-4xl mx-auto px-6 text-center">
          <div className="bg-gradient-to-br from-[#161b22] to-[#0d1117] border border-cyan-500/20 rounded-2xl p-12 shadow-xl shadow-cyan-900/10">
            <div className="inline-flex items-center space-x-1.5 text-emerald-400 text-xs font-mono mb-4">
              <CheckCircle2 className="h-4 w-4" />
              <span>Runs 100% locally — your issues never leave your servers</span>
            </div>
            <h2 className="text-3xl font-bold text-white mb-4">Ready to transform how your team handles issues?</h2>
            <p className="text-slate-400 mb-8">
              Connect your first GitHub repository in under 60 seconds. GitPilot begins learning your codebase's history immediately.
            </p>
            <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
              <Link
                to="/signup"
                className="group flex items-center space-x-2 px-6 py-3 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white font-semibold rounded-xl transition-all shadow-lg shadow-cyan-700/20 text-sm"
              >
                <span>Create Free Account</span>
                <ArrowRight className="h-4 w-4 group-hover:translate-x-1 transition-transform" />
              </Link>
              <div className="flex items-center space-x-1 text-xs text-slate-500 font-mono">
                <Star className="h-3.5 w-3.5 text-amber-400" />
                <span>No credit card required</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ── Footer ── */}
      <footer className="border-t border-[#21262d] py-10">
        <div className="max-w-6xl mx-auto px-6 flex flex-col md:flex-row items-center justify-between text-xs text-slate-500 font-mono gap-4">
          <div className="flex items-center space-x-2">
            <div className="h-5 w-5 rounded bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center">
              <Cpu className="h-3 w-3 text-white" />
            </div>
            <span>GitPilot AI — Engineering Intelligence Platform</span>
          </div>
          <div className="flex items-center space-x-4">
            <span>Local ML Engine</span>
            <span>·</span>
            <span>Zero External AI APIs</span>
            <span>·</span>
            <span>Open Architecture</span>
          </div>
        </div>
      </footer>
    </div>
  );
}
