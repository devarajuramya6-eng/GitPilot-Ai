import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Cpu, Mail, Lock, Eye, EyeOff, Loader2, ArrowRight, Terminal } from 'lucide-react';
import toast from 'react-hot-toast';
import { useAuthStore } from '../store/authStore';

export default function LoginPage() {
  const navigate = useNavigate();
  const { login } = useAuthStore();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email || !password) {
      toast.error('Please fill in all fields');
      return;
    }
    setLoading(true);
    try {
      const formData = new FormData();
      formData.append('username', email);
      formData.append('password', password);

      const res = await fetch('http://localhost:8000/api/auth/login', {
        method: 'POST',
        body: formData,
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Login failed');
      }

      const data = await res.json();
      // Decode user from token (simple approach)
      login(data.access_token, { id: 1, email });
      toast.success('Welcome back to GitPilot AI');
      navigate('/dashboard');
    } catch (err: any) {
      toast.error(err.message || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  };

  // Demo login
  const handleDemoLogin = () => {
    login('demo-token-12345', { id: 0, email: 'demo@gitpilot.ai' });
    toast.success('Entering Demo Mode — sample data loaded');
    navigate('/dashboard');
  };

  return (
    <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9] flex">
      {/* Left Panel: Form */}
      <div className="flex-1 flex flex-col justify-center items-center px-8 py-16">
        <div className="w-full max-w-sm">

          {/* Logo */}
          <Link to="/" className="flex items-center space-x-2.5 mb-10">
            <div className="h-9 w-9 rounded-xl bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
              <Cpu className="h-5 w-5 text-white" />
            </div>
            <span className="font-bold text-white text-lg tracking-wide">GitPilot AI</span>
          </Link>

          <h1 className="text-2xl font-bold text-white mb-1">Welcome back</h1>
          <p className="text-sm text-slate-400 mb-8">Sign in to your engineering control room</p>

          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Email */}
            <div>
              <label className="block text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-1.5">
                Email Address
              </label>
              <div className="relative">
                <Mail className="absolute left-3 top-2.5 h-4 w-4 text-slate-500" />
                <input
                  type="email"
                  value={email}
                  onChange={e => setEmail(e.target.value)}
                  placeholder="you@yourteam.dev"
                  className="w-full bg-[#161b22] border border-[#30363d] rounded-lg pl-9 pr-4 py-2.5 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500/30 transition-all"
                  autoComplete="email"
                />
              </div>
            </div>

            {/* Password */}
            <div>
              <div className="flex justify-between mb-1.5">
                <label className="block text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider">
                  Password
                </label>
                <button type="button" className="text-xs text-cyan-400 hover:text-cyan-300 transition-colors font-mono">
                  Forgot password?
                </button>
              </div>
              <div className="relative">
                <Lock className="absolute left-3 top-2.5 h-4 w-4 text-slate-500" />
                <input
                  type={showPassword ? 'text' : 'password'}
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  placeholder="Your secure password"
                  className="w-full bg-[#161b22] border border-[#30363d] rounded-lg pl-9 pr-10 py-2.5 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500/30 transition-all"
                  autoComplete="current-password"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-2.5 text-slate-500 hover:text-slate-300 transition-colors"
                >
                  {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
            </div>

            {/* Submit */}
            <button
              type="submit"
              disabled={loading}
              className="w-full flex items-center justify-center space-x-2 py-2.5 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 disabled:opacity-60 disabled:cursor-not-allowed text-white font-semibold rounded-lg transition-all text-sm shadow-md shadow-cyan-700/20 mt-2"
            >
              {loading ? (
                <><Loader2 className="h-4 w-4 animate-spin" /><span>Signing in…</span></>
              ) : (
                <><span>Sign In</span><ArrowRight className="h-4 w-4" /></>
              )}
            </button>
          </form>

          {/* Divider */}
          <div className="flex items-center my-5">
            <div className="flex-1 border-t border-[#30363d]" />
            <span className="px-3 text-xs text-slate-500 font-mono">or</span>
            <div className="flex-1 border-t border-[#30363d]" />
          </div>

          {/* Demo Login */}
          <button
            onClick={handleDemoLogin}
            className="w-full flex items-center justify-center space-x-2 py-2.5 bg-[#161b22] hover:bg-[#21262d] border border-[#30363d] text-slate-300 font-medium rounded-lg transition-all text-sm"
          >
            <Terminal className="h-4 w-4 text-cyan-400" />
            <span>Enter Demo Mode (no login required)</span>
          </button>

          {/* Sign up */}
          <p className="text-center text-xs text-slate-500 mt-8">
            No account?{' '}
            <Link to="/signup" className="text-cyan-400 hover:text-cyan-300 font-medium transition-colors">
              Create one free →
            </Link>
          </p>
        </div>
      </div>

      {/* Right Panel: Info */}
      <div className="hidden lg:flex w-[420px] bg-[#161b22] border-l border-[#30363d] flex-col justify-center p-12 relative overflow-hidden">
        {/* Background grid */}
        <div
          className="absolute inset-0 opacity-[0.04]"
          style={{
            backgroundImage: 'linear-gradient(#c9d1d9 1px, transparent 1px), linear-gradient(to right, #c9d1d9 1px, transparent 1px)',
            backgroundSize: '32px 32px',
          }}
        />
        <div className="absolute top-0 right-0 h-64 w-64 bg-cyan-600/10 rounded-full blur-3xl" />

        <div className="relative">
          <div className="inline-flex items-center space-x-2 text-xs font-mono text-emerald-400 mb-8">
            <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
            <span>Local ML Engine Active — 0 external API calls</span>
          </div>

          {/* Live triage mock */}
          <div className="space-y-3">
            {[
              { num: '#145', type: 'Security', priority: 'Critical', dev: 'Anil K.', match: '94%', time: '18ms', cls: 'text-red-400' },
              { num: '#146', type: 'Bug', priority: 'High', dev: 'Elena R.', match: '89%', time: '21ms', cls: 'text-amber-400' },
              { num: '#147', type: 'Feature', priority: 'Medium', dev: 'Marcus V.', match: '86%', time: '15ms', cls: 'text-blue-400' },
            ].map(item => (
              <div key={item.num} className="bg-[#0d1117] border border-[#30363d] rounded-lg p-3 font-mono text-xs">
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-cyan-400 font-semibold">{item.num}</span>
                  <span className={`font-semibold ${item.cls}`}>{item.priority}</span>
                </div>
                <div className="flex items-center justify-between text-slate-400">
                  <span>{item.type}</span>
                  <span className="text-emerald-400">{item.dev} ({item.match})</span>
                </div>
                <div className="text-slate-600 mt-1">Inference: {item.time}</div>
              </div>
            ))}
          </div>

          <div className="mt-8 p-4 bg-[#0d1117] border border-[#30363d] rounded-lg">
            <p className="text-xs text-slate-400 leading-relaxed font-mono">
              <span className="text-cyan-300 font-semibold">Repository Memory:</span> GitPilot learns from your project's history — commits, resolutions, and developer patterns — to improve every recommendation over time.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
