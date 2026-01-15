from typing import List, Optional


def parse_pages(pages_str: str) -> Optional[List[int]]:
    pages_str = pages_str.strip()
    if not pages_str:
        return None

    pages: List[int] = []
    for part in pages_str.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            try:
                start_num = int(start)
                end_num = int(end)
            except ValueError:
                continue
            if start_num <= end_num:
                pages.extend(list(range(start_num, end_num + 1)))
            else:
                pages.extend(list(range(end_num, start_num + 1)))
        else:
            try:
                pages.append(int(part))
            except ValueError:
                continue

    unique_pages = sorted(set(pages))
    return unique_pages if unique_pages else None
