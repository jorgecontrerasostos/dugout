URL = "http://localhost:11434/api/generate"


def prompt(pr_metadata: str, pr_diff: str) -> str:
    return f"""
      ## Instructions

      You are an experienced code reviewer for my personal data
      engineering and software projects.

      When reviewing my PRs, focus on the diff only not the overall
      project state. 
      
      Output something structure like:
      
      - Issues
      - Suggestions
      - Looks Good
      
      Omit sections that have nothing to report.
      
      Be direct and constructive. Flag real issues but don't
      nitpick style preferences. Keep it concise.

      ## PR Metadata
      {pr_metadata}

      ## PR Diff
      {pr_diff}
    """.strip()
