import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Cpu, Mail, Lock, User, Eye, EyeOff, Loader2, ArrowRight, CheckCircle2 } from 'lucide-react';
import toast from 'react-hot-toast';
import { useAuthStore } from '../store/authStore';

const PERKS = [
  'Connect unlimited GitHub repositories',
  'Local ML runs on your infrastructure',
  'Repository Memory Engine included',
  'Human-in-the-loop approval workflow',
  'Engineering Health Score dashboard',
  'No credit card required',
];

export default function SignupPage() {
  const navigate = useNavigate();
  const { login } = useAuthStore();
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name || !email || !password) {
      toast.error('Please fill in all fields');
      return;
    }
    if (password.length < 8) {
      toast.error('Password must be at least 8 characters');
      return;
    }
    setLoading(true);
    try {
      const res = await fetch('http://localhost:8000/api/auth/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || 'Registration failed');
      }

      // Auto-login after registration
      const formData = new FormData();
      formData.append('username', email);
      formData.append('password', password);
      const loginRes = await fetch('http://localhost:8000/api/auth/login', {
        method: 'POST',
        body: formData,
      });
      const loginData = await loginRes.json();
      login(loginData.access_token, { id: 1, email });
      toast.success('Account created! Welcome to GitPilot AI.');
      navigate('/connect');
    } catch (err: any) {
      toast.error(err.message || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  };

  const strength = (() => {
    if (password.length === 0) return 0;
    let s = 0;
    if (password.length >= 8) s++;
    if (/[A-Z]/.test(password)) s++;
    if (/[0-9]/.test(password)) s++;
    if (/[^A-Za-z0-9]/.test(password)) s++;
    return s;
  })();

  const strengthLabel = ['', 'Weak', 'Fair', 'Good', 'Strong'];
  const strengthColor = ['', 'bg-red-500', 'bg-amber-500', 'bg-blue-500', 'bg-emerald-500'];

  return (
    <div className="min-h-screen bg-[#0d1117] text-[#c9d1d9] flex">
      {/* Left: Perks panel */}
      <div className="hidden lg:flex w-[380px] bg-[#161b22] border-r border-[#30363d] flex-col justify-center p-12 relative overflow-hidden">
        <div
          className="absolute inset-0 opacity-[0.04]"
          style={{
            backgroundImage: 'linear-gradient(#c9d1d9 1px, transparent 1px), linear-gradient(to right, #c9d1d9 1px, transparent 1px)',
            backgroundSize: '32px 32px',
          }}
        />
        <div className="absolute bottom-0 left-0 h-64 w-64 bg-cyan-600/10 rounded-full blur-3xl" />

        <div className="relative">
          <div className="h-12 w-12 rounded-2xl bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center shadow-xl shadow-cyan-500/20 mb-8">
            <Cpu className="h-6 w-6 text-white" />
          </div>
          <h2 className="text-xl font-bold text-white mb-2">Start triaging smarter</h2>
          <p className="text-sm text-slate-400 leading-relaxed mb-8">
            Join teams who've replaced chaotic GitHub issue queues with an intelligent, locally-powered triage engine.
          </p>
          <ul className="space-y-3">
            {PERKS.map(p => (
              <li key={p} className="flex items-center space-x-3 text-sm text-slate-300">
                <CheckCircle2 className="h-4 w-4 text-emerald-400 flex-shrink-0" />
                <span>{p}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>

      {/* Right: Form */}
      <div className="flex-1 flex flex-col justify-center items-center px-8 py-16">
        <div className="w-full max-w-sm">
          <Link to="/" className="flex items-center space-x-2.5 mb-10">
            <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
              <Cpu className="h-4 w-4 text-white" />
            </div>
            <span className="font-bold text-white tracking-wide">GitPilot AI</span>
          </Link>

          <h1 className="text-2xl font-bold text-white mb-1">Create your account</h1>
          <p className="text-sm text-slate-400 mb-8">Free forever for open-source repositories</p>

          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Name */}
            <div>
              <label className="block text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-1.5">
                Full Name
              </label>
              <div className="relative">
                <User className="absolute left-3 top-2.5 h-4 w-4 text-slate-500" />
                <input
                  type="text"
                  value={name}
                  onChange={e => setName(e.target.value)}
                  placeholder="Your name"
                  className="w-full bg-[#161b22] border border-[#30363d] rounded-lg pl-9 pr-4 py-2.5 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500/30 transition-all"
                />
              </div>
            </div>

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
              <label className="block text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-1.5">
                Password
              </label>
              <div className="relative">
                <Lock className="absolute left-3 top-2.5 h-4 w-4 text-slate-500" />
                <input
                  type={showPassword ? 'text' : 'password'}
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  placeholder="Min. 8 characters"
                  className="w-full bg-[#161b22] border border-[#30363d] rounded-lg pl-9 pr-10 py-2.5 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500/30 transition-all"
                  autoComplete="new-password"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-2.5 text-slate-500 hover:text-slate-300 transition-colors"
                >
                  {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
              {/* Password strength bar */}
              {password.length > 0 && (
                <div className="mt-2">
                  <div className="flex space-x-1 mb-1">
                    {[1, 2, 3, 4].map(i => (
                      <div
                        key={i}
                        className={`h-1 flex-1 rounded-full transition-all ${i <= strength ? strengthColor[strength] : 'bg-[#30363d]'}`}
                      />
                    ))}
                  </div>
                  <span className="text-xs font-mono text-slate-500">{strengthLabel[strength]}</span>
                </div>
              )}
            </div>

            {/* Terms */}
            <p className="text-xs text-slate-500 leading-relaxed">
              By creating an account you agree that GitPilot AI operates entirely locally.
              Your repository data is never sent to external AI services.
            </p>

            {/* Submit */}
            <button
              type="submit"
              disabled={loading}
              className="w-full flex items-center justify-center space-x-2 py-2.5 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 disabled:opacity-60 disabled:cursor-not-allowed text-white font-semibold rounded-lg transition-all text-sm shadow-md shadow-cyan-700/20"
            >
              {loading ? (
                <><Loader2 className="h-4 w-4 animate-spin" /><span>Creating account…</span></>
              ) : (
                <><span>Create Account & Connect GitHub</span><ArrowRight className="h-4 w-4" /></>
              )}
            </button>
          </form>

          <p className="text-center text-xs text-slate-500 mt-8">
            Already have an account?{' '}
            <Link to="/login" className="text-cyan-400 hover:text-cyan-300 font-medium transition-colors">
              Sign in →
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}
