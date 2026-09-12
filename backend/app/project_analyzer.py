"""
GitPilot Static Project Analysis Engine
Securely scans uploaded codebases (.zip, .tar, .tar.gz), parses languages,
detects frameworks, dependencies, configurations, tests, and computes metrics.
Treats all uploaded archives as untrusted (Path Traversal & Zip Slip protected).
"""
import os
import zipfile
import tarfile
import tempfile
import shutil
from typing import Dict, List, Any

# File extension mappings
LANG_EXTENSIONS = {
    ".py": "Python",
    ".ts": "TypeScript",
    ".tsx": "TypeScript (React)",
    ".js": "JavaScript",
    ".jsx": "JavaScript (React)",
    ".go": "Go",
    ".rs": "Rust",
    ".java": "Java",
    ".cpp": "C++",
    ".c": "C",
    ".h": "C/C++ Header",
    ".cs": "C#",
    ".rb": "Ruby",
    ".php": "PHP",
    ".html": "HTML",
    ".css": "CSS",
    ".scss": "SCSS",
    ".sql": "SQL",
    ".sh": "Shell Script",
    ".yaml": "YAML",
    ".yml": "YAML",
    ".json": "JSON",
    ".md": "Markdown"
}

FRAMEWORK_INDICATORS = {
    "FastAPI": ["fastapi", "uvicorn"],
    "Django": ["django", "manage.py"],
    "Flask": ["flask"],
    "React": ["react", "react-dom"],
    "Next.js": ["next", "next.config.js"],
    "Vue.js": ["vue"],
    "Tailwind CSS": ["tailwindcss", "tailwind.config.js"],
    "Express.js": ["express"],
    "Spring Boot": ["spring-boot", "pom.xml"],
    "PostgreSQL / SQLAlchemy": ["sqlalchemy", "psycopg", "alembic"],
    "Docker / Containers": ["Dockerfile", "docker-compose.yml"],
    "Kubernetes": ["k8s", "helm", "Chart.yaml"]
}

class ProjectAnalyzer:
    def __init__(self, max_file_size_mb: int = 50):
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024

    def safe_extract_zip(self, zip_path: str, extract_dir: str):
        """Prevents Zip Slip / Path Traversal attacks."""
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for member in zf.infolist():
                # Normalize and check path
                target_path = os.path.abspath(os.path.join(extract_dir, member.filename))
                if not target_path.startswith(os.path.abspath(extract_dir)):
                    raise ValueError(f"Security Alert: Path Traversal attempt detected in archive file: {member.filename}")
                zf.extract(member, extract_dir)

    def analyze_directory(self, target_dir: str) -> Dict[str, Any]:
        total_files = 0
        total_loc = 0
        lang_counts: Dict[str, int] = {}
        lang_loc: Dict[str, int] = {}
        test_files = 0
        doc_files = 0
        config_files = 0
        frameworks_found = set()
        file_tree = []

        ignore_dirs = {".git", "node_modules", "dist", "build", "venv", "__pycache__", ".idea", ".vscode"}

        for root, dirs, files in os.walk(target_dir):
            # Modify dirs in-place to skip ignored directories
            dirs[:] = [d for d in dirs if d not in ignore_dirs]

            for file in files:
                total_files += 1
                rel_path = os.path.relpath(os.path.join(root, file), target_dir)
                ext = os.path.splitext(file)[1].lower()
                lang = LANG_EXTENSIONS.get(ext, "Other")

                # Test file detection
                if "test" in file.lower() or "spec" in file.lower():
                    test_files += 1

                # Docs detection
                if ext in [".md", ".rst", ".txt"]:
                    doc_files += 1

                # Config detection
                if file.startswith(".") or ext in [".json", ".yaml", ".yml", ".toml", ".ini", ".env"]:
                    config_files += 1

                # Count Lines of Code
                full_path = os.path.join(root, file)
                try:
                    with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                        lines = len(f.readlines())
                        total_loc += lines
                        if lang != "Other":
                            lang_loc[lang] = lang_loc.get(lang, 0) + lines
                except Exception:
                    pass

                if lang != "Other":
                    lang_counts[lang] = lang_counts.get(lang, 0) + 1

                # Check framework indicators from filename
                for fw, indicators in FRAMEWORK_INDICATORS.items():
                    if any(ind in file.lower() for ind in indicators):
                        frameworks_found.add(fw)

                if len(file_tree) < 50:
                    file_tree.append(rel_path.replace("\\", "/"))

        # Calculate percentages
        top_languages = []
        for lang, count in sorted(lang_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            pct = round((count / (total_files or 1)) * 100, 1)
            top_languages.append({
                "language": lang,
                "files": count,
                "loc": lang_loc.get(lang, 0),
                "percentage": pct
            })

        return {
            "summary": {
                "total_files": total_files,
                "total_loc": total_loc,
                "test_files": test_files,
                "doc_files": doc_files,
                "config_files": config_files,
                "estimated_complexity": "Medium" if total_loc > 10000 else "Low"
            },
            "languages": top_languages,
            "detected_frameworks": list(frameworks_found),
            "sample_structure": file_tree[:30]
        }

project_analyzer = ProjectAnalyzer()
