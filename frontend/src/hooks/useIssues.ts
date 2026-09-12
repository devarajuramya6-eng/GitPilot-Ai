/**
 * GitPilot AI — Issue Management Hooks
 * Provides clean custom hooks for issues, triage, filtering, and assignment.
 */
import { useState, useEffect, useCallback, useMemo } from 'react';

export interface IssueItem {
  id: string;
  number: number;
  title: string;
  description: string;
  repo: string;
  type: string;
  priority: string;
  complexity: string;
  component: string;
  duplicateProb: number;
  qualityScore: number;
  recommendedDev: {
    name: string;
    score: number;
    workload: string;
    matchReason?: string;
  };
  explanation?: string[];
  status: string;
  timestamp: string;
  labels?: string[];
}

export function useIssues() {
  const [issues, setIssues] = useState<IssueItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchIssues = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('http://localhost:8000/api/issues');
      if (!res.ok) throw new Error(`Failed to fetch issues: ${res.statusText}`);
      const data = await res.json();
      setIssues(data);
    } catch (err: any) {
      setError(err.message || 'Error fetching issues');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchIssues();
  }, [fetchIssues]);

  const addIssue = useCallback((newIssue: IssueItem) => {
    setIssues(prev => [newIssue, ...prev]);
  }, []);

  const updateIssueStatus = useCallback((id: string | number, newStatus: string) => {
    setIssues(prev => prev.map(issue => 
      (issue.id === id || issue.number === id) ? { ...issue, status: newStatus } : issue
    ));
  }, []);

  return {
    issues,
    loading,
    error,
    refetch: fetchIssues,
    addIssue,
    updateIssueStatus,
  };
}

export function useIssueFilter(issues: IssueItem[], initialFilter: string = 'All') {
  const [filter, setFilter] = useState(initialFilter);
  const [search, setSearch] = useState('');

  const filteredIssues = useMemo(() => {
    return issues
      .filter(item => filter === 'All' || item.type === filter || item.priority === filter)
      .filter(item => 
        !search.trim() || 
        item.title.toLowerCase().includes(search.toLowerCase()) ||
        item.component.toLowerCase().includes(search.toLowerCase()) ||
        item.repo.toLowerCase().includes(search.toLowerCase())
      );
  }, [issues, filter, search]);

  return {
    filter,
    setFilter,
    search,
    setSearch,
    filteredIssues,
  };
}
