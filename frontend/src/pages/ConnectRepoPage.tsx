import { useState, useRef } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import {
  Cpu, Upload, FileCode, CheckCircle2, AlertCircle,
  Loader2, ArrowRight, FolderGit2, Globe, Lock, ChevronRight,
  BookOpen, Star, GitBranch, ExternalLink, X
} from 'lucide-react';
import toast from 'react-hot-toast';
import { useAuthStore } from '../store/authStore';

const POPULAR_REPOS = [
  { name: 'acme/auth-core-service', issues: 142, stars: 1240, language: 'Python', private: false },
  { name: 'acme/gateway-api', issues: 87, stars: 890, language: 'Go', private: false },
  { name: 'acme/console-ui', issues: 204, stars: 560, language: 'TypeScript', private: true },
  { name: 'acme/analytics-pipeline', issues: 63, stars: 340, language: 'Python', private: false },
];

const LANG_COLORS: Record<string, string> = {
  Python: 'bg-blue-400',
  Go: 'bg-cyan-400',
  TypeScript: 'bg-amber-400',
  JavaScript: 'bg-yellow-400',
  Rust: 'bg-orange-400',
};

type ConnectMode = 'github' | 'url' | 'upload';

interface FileUploadState {
  file: File | null;
  preview: { name: string; size: string; issues?: number } | null;
  parsing: boolean;
}

export default function ConnectRepoPage() {
  const navigate = useNavigate();
  const { user } = useAuthStore();
  const [mode, setMode] = useState<ConnectMode>('github');
  const [repoUrl, setRepoUrl] = useState('');
  const [token, setToken] = useState('');
  const [connecting, setConnecting] = useState(false);
  const [connectedRepos, setConnectedRepos] = useState<string[]>([]);
  const [upload, setUpload] = useState<FileUploadState>({ file: null, preview: null, parsing: false });
  const [dragOver, setDragOver] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  const handleConnectRepo = async (repoName: string) => {
    if (connectedRepos.includes(repoName)) return;
    setConnecting(true);
    await new Promise(r => setTimeout(r, 1200));
    setConnectedRepos(prev => [...prev, repoName]);
    toast.success(`Connected ${repoName} — syncing issue history...`);
    setConnecting(false);
  };

  const handleUrlConnect = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!repoUrl.trim()) { toast.error('Enter a GitHub repository URL'); return; }
    if (!repoUrl.includes('github.com')) { toast.error('Must be a valid github.com URL'); return; }
    setConnecting(true);
    await new Promise(r => setTimeout(r, 1500));
    const repoName = repoUrl.replace('https://github.com/', '').replace(/\/$/, '');
    setConnectedRepos(prev => [...prev, repoName]);
    toast.success(`Repository "${repoName}" connected successfully!`);
    setConnecting(false);
    setRepoUrl('');
  };

  const handleFileDrop = (files: FileList | null) => {
    if (!files || files.length === 0) return;
    const file = files[0];
    if (!file.name.endsWith('.json') && !file.name.endsWith('.csv')) {
      toast.error('Only .json or .csv issue export files are supported');
      return;
    }
    setUpload({ file, preview: null, parsing: true });
    setTimeout(() => {
      setUpload({
        file,
        preview: {
          name: file.name,
          size: (file.size / 1024).toFixed(1) + ' KB',
          issues: Math.floor(Math.random() * 200) + 50,
        },
        parsing: false,
      });
      toast.success(`Parsed "${file.name}" — ready to import`);
    }, 1200);
  };

  const handleImportFile = () => {
    if (!upload.preview) return;
    setConnecting(true);
    setTimeout(() => {
      setConnecting(false);
      toast.success(`Imported ${upload.preview!.issues} issues — ML pipeline training started`);
      setUpload({ file: null, preview: null, parsing: false });
    }, 1800);
  };

  return (
    <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9]">

      {/* Top Nav */}
      <nav className="border-b border-[#21262d] bg-[#161b22] px-6 py-3 flex items-center justify-between">
        <Link to="/" className="flex items-center space-x-2.5">
          <div className="h-7 w-7 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center">
            <Cpu className="h-4 w-4 text-white" />
          </div>
          <span className="font-bold text-white text-sm tracking-wide">GitPilot AI</span>
        </Link>
        <div className="flex items-center space-x-3 text-sm">
          <span className="text-slate-400 font-mono text-xs">Signed in as <span className="text-cyan-400">{user?.email}</span></span>
          <Link to="/dashboard" className="px-3 py-1.5 bg-[#21262d] hover:bg-[#30363d] border border-[#30363d] rounded-lg text-slate-200 text-xs font-medium transition-all flex items-center space-x-1.5">
            <span>Skip to Dashboard</span>
            <ArrowRight className="h-3.5 w-3.5" />
          </Link>
        </div>
      </nav>

      {/* Main Content */}
      <div className="max-w-5xl mx-auto px-6 py-12">

        {/* Header */}
        <div className="text-center mb-10">
          <div className="inline-flex items-center space-x-2 px-3 py-1 rounded-full bg-[#161b22] border border-[#30363d] text-xs font-mono text-emerald-400 mb-4">
            <CheckCircle2 className="h-3.5 w-3.5" />
            <span>Step 1 of 2 — Connect your repository</span>
          </div>
          <h1 className="text-3xl font-bold text-white mb-3">Connect a GitHub Repository</h1>
          <p className="text-slate-400 max-w-xl mx-auto text-sm leading-relaxed">
            GitPilot AI will sync issue history, commits, and contributor data to train your local ML models. All data stays on your machine.
          </p>
        </div>

        {/* Mode Selector */}
        <div className="flex justify-center mb-8">
          <div className="flex bg-[#161b22] border border-[#30363d] rounded-xl p-1 space-x-1">
            {[
              { id: 'github', label: 'GitHub OAuth', icon: FolderGit2 },
              { id: 'url', label: 'Paste URL', icon: Globe },
              { id: 'upload', label: 'Upload Export', icon: Upload },
            ].map(m => {
              const Icon = m.icon;
              return (
                <button
                  key={m.id}
                  onClick={() => setMode(m.id as ConnectMode)}
                  className={`flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-medium transition-all ${
                    mode === m.id
                      ? 'bg-[#21262d] text-white border border-[#30363d] shadow-sm'
                      : 'text-slate-400 hover:text-slate-200'
                  }`}
                >
                  <Icon className="h-3.5 w-3.5 text-cyan-400" />
                  <span>{m.label}</span>
                </button>
              );
            })}
          </div>
        </div>

        <div className="grid lg:grid-cols-5 gap-6">

          {/* Left: Main connect panel */}
          <div className="lg:col-span-3 space-y-4">

            {/* GitHub OAuth Mode */}
            {mode === 'github' && (
              <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6">
                <div className="flex items-center space-x-3 mb-5">
                  <div className="h-10 w-10 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center">
                    <FolderGit2 className="h-5 w-5 text-white" />
                  </div>
                  <div>
                    <h2 className="font-semibold text-white text-sm">GitHub Repository Access</h2>
                    <p className="text-xs text-slate-400">Select from your connected repositories or provide a token</p>
                  </div>
                </div>

                <div className="mb-4">
                  <label className="block text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-1.5">
                    GitHub Personal Access Token (optional)
                  </label>
                  <div className="relative">
                    <Lock className="absolute left-3 top-2.5 h-4 w-4 text-slate-500" />
                    <input
                      type="password"
                      value={token}
                      onChange={e => setToken(e.target.value)}
                      placeholder="ghp_xxxxxxxxxxxxxxxxxxxx"
                      className="w-full bg-[#0d1117] border border-[#30363d] rounded-lg pl-9 pr-4 py-2.5 text-xs text-white placeholder-slate-600 focus:outline-none focus:border-cyan-500 transition-all font-mono"
                    />
                  </div>
                  <p className="text-[11px] text-slate-500 mt-1.5 font-mono">
                    Requires <code className="text-cyan-400">repo</code> and <code className="text-cyan-400">read:user</code> scopes.
                    Token is stored locally, never transmitted externally.
                  </p>
                </div>

                <div className="text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-3">
                  Available Repositories
                </div>
                <div className="space-y-2">
                  {POPULAR_REPOS.map(repo => {
                    const isConnected = connectedRepos.includes(repo.name);
                    return (
                      <div
                        key={repo.name}
                        className={`flex items-center justify-between p-3 rounded-lg border transition-all ${
                          isConnected
                            ? 'bg-emerald-950/20 border-emerald-700/40'
                            : 'bg-[#0d1117] border-[#30363d] hover:border-[#484f58]'
                        }`}
                      >
                        <div className="flex items-center space-x-3">
                          <FolderGit2 className={`h-4 w-4 ${isConnected ? 'text-emerald-400' : 'text-slate-500'}`} />
                          <div>
                            <div className="flex items-center space-x-2">
                              <span className="text-xs font-semibold text-white">{repo.name}</span>
                              {repo.private && (
                                <span className="text-[10px] font-mono px-1.5 py-0.5 bg-[#21262d] border border-[#30363d] text-slate-400 rounded">Private</span>
                              )}
                            </div>
                            <div className="flex items-center space-x-3 mt-0.5 text-[11px] text-slate-500 font-mono">
                              <span className="flex items-center gap-1">
                                <span className={`h-2 w-2 rounded-full ${LANG_COLORS[repo.language] || 'bg-slate-400'}`} />
                                {repo.language}
                              </span>
                              <span className="flex items-center gap-1"><Star className="h-3 w-3" />{repo.stars}</span>
                              <span className="flex items-center gap-1"><BookOpen className="h-3 w-3" />{repo.issues} issues</span>
                            </div>
                          </div>
                        </div>
                        <button
                          onClick={() => handleConnectRepo(repo.name)}
                          disabled={connecting || isConnected}
                          className={`flex items-center space-x-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                            isConnected
                              ? 'bg-emerald-950 text-emerald-400 border border-emerald-800/60 cursor-default'
                              : 'bg-[#21262d] hover:bg-cyan-950 text-slate-300 hover:text-cyan-400 border border-[#30363d] hover:border-cyan-800'
                          }`}
                        >
                          {isConnected ? (
                            <><CheckCircle2 className="h-3.5 w-3.5" /><span>Connected</span></>
                          ) : connecting ? (
                            <><Loader2 className="h-3.5 w-3.5 animate-spin" /><span>Connecting…</span></>
                          ) : (
                            <><ChevronRight className="h-3.5 w-3.5" /><span>Connect</span></>
                          )}
                        </button>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* URL Mode */}
            {mode === 'url' && (
              <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6">
                <div className="flex items-center space-x-3 mb-5">
                  <div className="h-10 w-10 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center">
                    <Globe className="h-5 w-5 text-cyan-400" />
                  </div>
                  <div>
                    <h2 className="font-semibold text-white text-sm">Connect by Repository URL</h2>
                    <p className="text-xs text-slate-400">Paste any public GitHub repository URL</p>
                  </div>
                </div>
                <form onSubmit={handleUrlConnect} className="space-y-4">
                  <div>
                    <label className="block text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-1.5">
                      GitHub Repository URL
                    </label>
                    <div className="relative">
                      <Globe className="absolute left-3 top-2.5 h-4 w-4 text-slate-500" />
                      <input
                        type="url"
                        value={repoUrl}
                        onChange={e => setRepoUrl(e.target.value)}
                        placeholder="https://github.com/owner/repository"
                        className="w-full bg-[#0d1117] border border-[#30363d] rounded-lg pl-9 pr-4 py-2.5 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-cyan-500 transition-all"
                      />
                    </div>
                  </div>
                  <button
                    type="submit"
                    disabled={connecting}
                    className="w-full flex items-center justify-center space-x-2 py-2.5 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 disabled:opacity-60 text-white font-semibold rounded-lg transition-all text-sm"
                  >
                    {connecting ? <><Loader2 className="h-4 w-4 animate-spin" /><span>Connecting…</span></> : <><GitBranch className="h-4 w-4" /><span>Connect Repository</span></>}
                  </button>
                </form>
              </div>
            )}

            {/* Upload Mode */}
            {mode === 'upload' && (
              <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-6">
                <div className="flex items-center space-x-3 mb-5">
                  <div className="h-10 w-10 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center">
                    <Upload className="h-5 w-5 text-purple-400" />
                  </div>
                  <div>
                    <h2 className="font-semibold text-white text-sm">Upload Issue Export File</h2>
                    <p className="text-xs text-slate-400">Import existing issue data from JSON or CSV export</p>
                  </div>
                </div>

                {/* Drop zone */}
                <div
                  className={`border-2 border-dashed rounded-xl p-10 text-center transition-all cursor-pointer ${
                    dragOver ? 'border-cyan-500 bg-cyan-950/20' : 'border-[#30363d] hover:border-[#484f58] bg-[#0d1117]'
                  }`}
                  onDragOver={e => { e.preventDefault(); setDragOver(true); }}
                  onDragLeave={() => setDragOver(false)}
                  onDrop={e => { e.preventDefault(); setDragOver(false); handleFileDrop(e.dataTransfer.files); }}
                  onClick={() => fileRef.current?.click()}
                >
                  <input
                    ref={fileRef}
                    type="file"
                    accept=".json,.csv"
                    className="hidden"
                    onChange={e => handleFileDrop(e.target.files)}
                  />
                  {upload.parsing ? (
                    <div className="flex flex-col items-center space-y-3">
                      <Loader2 className="h-8 w-8 text-cyan-400 animate-spin" />
                      <span className="text-sm text-slate-400 font-mono">Parsing issue data…</span>
                    </div>
                  ) : upload.preview ? (
                    <div className="flex flex-col items-center space-y-3">
                      <div className="h-12 w-12 rounded-xl bg-emerald-950 border border-emerald-700/60 flex items-center justify-center">
                        <FileCode className="h-6 w-6 text-emerald-400" />
                      </div>
                      <div>
                        <div className="text-sm font-semibold text-white">{upload.preview.name}</div>
                        <div className="text-xs text-slate-400 font-mono">{upload.preview.size} · {upload.preview.issues} issues detected</div>
                      </div>
                      <button
                        onClick={e => { e.stopPropagation(); setUpload({ file: null, preview: null, parsing: false }); }}
                        className="text-xs text-rose-400 hover:text-rose-300 flex items-center space-x-1"
                      >
                        <X className="h-3.5 w-3.5" /><span>Remove file</span>
                      </button>
                    </div>
                  ) : (
                    <div className="flex flex-col items-center space-y-3">
                      <div className="h-12 w-12 rounded-xl bg-[#21262d] border border-[#30363d] flex items-center justify-center">
                        <Upload className="h-6 w-6 text-slate-400" />
                      </div>
                      <div>
                        <div className="text-sm text-white font-medium mb-1">Drop your export file here</div>
                        <div className="text-xs text-slate-500 font-mono">Supports GitHub issue export (.json), Jira export (.csv)</div>
                      </div>
                      <div className="text-xs text-cyan-400 font-mono">or click to browse</div>
                    </div>
                  )}
                </div>

                {upload.preview && (
                  <button
                    onClick={handleImportFile}
                    disabled={connecting}
                    className="w-full flex items-center justify-center space-x-2 py-2.5 mt-4 bg-gradient-to-r from-purple-600 to-cyan-600 hover:from-purple-500 hover:to-cyan-500 disabled:opacity-60 text-white font-semibold rounded-lg transition-all text-sm"
                  >
                    {connecting ? <><Loader2 className="h-4 w-4 animate-spin" /><span>Importing…</span></> : <><Upload className="h-4 w-4" /><span>Import {upload.preview.issues} Issues → Train Local ML</span></>}
                  </button>
                )}

                <div className="mt-4 p-3 bg-[#0d1117] border border-[#30363d] rounded-lg text-xs text-slate-400 font-mono flex items-start space-x-2">
                  <AlertCircle className="h-4 w-4 text-amber-400 flex-shrink-0 mt-0.5" />
                  <span>Uploaded files are processed entirely on your local machine. No data is transmitted to external servers.</span>
                </div>
              </div>
            )}
          </div>

          {/* Right: Status sidebar */}
          <div className="lg:col-span-2 space-y-4">

            {/* Connection Status */}
            <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-5">
              <div className="text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-4">Connection Status</div>
              {connectedRepos.length === 0 ? (
                <div className="text-center py-6">
                  <FolderGit2 className="h-8 w-8 text-slate-600 mx-auto mb-2" />
                  <p className="text-xs text-slate-500">No repositories connected yet</p>
                </div>
              ) : (
                <div className="space-y-2.5">
                  {connectedRepos.map(repo => (
                    <div key={repo} className="flex items-center justify-between text-xs">
                      <div className="flex items-center space-x-2">
                        <CheckCircle2 className="h-3.5 w-3.5 text-emerald-400 flex-shrink-0" />
                        <span className="text-slate-300 font-mono truncate max-w-[160px]">{repo}</span>
                      </div>
                      <span className="text-emerald-400 font-mono text-[10px]">Syncing…</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* What happens next */}
            <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-5">
              <div className="text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-4">What happens next</div>
              <ul className="space-y-3 text-xs text-slate-400">
                {[
                  { icon: GitBranch, text: 'Issues, commits, and contributor history synced' },
                  { icon: Cpu, text: 'TF-IDF models trained on your repository vocabulary' },
                  { icon: CheckCircle2, text: 'Developer profiles built from commit patterns' },
                  { icon: ExternalLink, text: 'Webhook registered for real-time issue triage' },
                ].map(step => {
                  const Icon = step.icon;
                  return (
                    <li key={step.text} className="flex items-start space-x-2.5">
                      <Icon className="h-3.5 w-3.5 text-cyan-400 flex-shrink-0 mt-0.5" />
                      <span>{step.text}</span>
                    </li>
                  );
                })}
              </ul>
            </div>

            {/* Proceed Button */}
            {connectedRepos.length > 0 && (
              <button
                onClick={() => { toast.success('Proceeding to dashboard…'); navigate('/dashboard'); }}
                className="w-full flex items-center justify-center space-x-2 py-3 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white font-semibold rounded-xl transition-all text-sm shadow-lg shadow-cyan-700/20"
              >
                <span>Open Engineering Control Room</span>
                <ArrowRight className="h-4 w-4" />
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
