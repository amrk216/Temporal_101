import textwrap

_SUMMARY_PROMPT = textwrap.dedent("""\
    You are a legal analyst reviewing a contract excerpt.

    Identify the key obligations, rights, and risks for the parties involved.

    Return ONLY a JSON object with exactly these two fields — no markdown, no code block:
    {{
      "summary": "2-3 sentence plain-English summary of what this contract covers and the main obligations of each party",
      "key_risks": "bullet list of the top 3-5 risks, one per line, starting with a dash (e.g. - Risk description)"
    }}

    Contract text:
    {text}
                                  
    # Output:
    
    ```json                              
    """)
