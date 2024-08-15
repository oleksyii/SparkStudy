import re

text = "It's a bummer"
pattern = r"\b\w+(?:'\w+)?\b"
matches = re.findall(pattern, text)

print(matches)
