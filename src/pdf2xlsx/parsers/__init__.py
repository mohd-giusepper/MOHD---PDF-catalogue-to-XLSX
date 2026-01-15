from typing import Dict, Type

from pdf2xlsx import config
from pdf2xlsx.parsers.base import BaseParser
from pdf2xlsx.parsers.stelton_2025 import Stelton2025Parser


REGISTRY: Dict[str, Type[BaseParser]] = {
    "stelton_2025": Stelton2025Parser,
}


def get_parser(name: str) -> BaseParser:
    parser_name = name or config.DEFAULT_PARSER
    if parser_name not in REGISTRY:
        raise ValueError(f"Unknown parser: {parser_name}")
    return REGISTRY[parser_name]()
