"""Portfolio report generation module.

Provides :func:`generate_report` (entry point for the CLI) and
:func:`render_report` (orchestration + HTML output).
"""

from pipeline.report.renderer import generate_report, render_report

__all__ = ["generate_report", "render_report"]
