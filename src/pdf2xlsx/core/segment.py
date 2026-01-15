from typing import Callable, List


def split_blocks(
    lines: List[str],
    is_block_boundary: Callable[[List[str], int, bool], bool],
    contains_art_no: Callable[[str], bool],
    is_block_valid: Callable[[str], bool],
) -> List[str]:
    blocks: List[List[str]] = []
    current: List[str] = []
    have_art = False

    for idx, line in enumerate(lines):
        if is_block_boundary(lines, idx, have_art):
            if current:
                blocks.append(current)
            current = []
            have_art = False

        current.append(line)
        if contains_art_no(line):
            have_art = True

    if current:
        blocks.append(current)

    filtered = []
    for block_lines in blocks:
        block_text = "\n".join([line for line in block_lines if line.strip()])
        if is_block_valid(block_text):
            filtered.append(block_text)
    return filtered
