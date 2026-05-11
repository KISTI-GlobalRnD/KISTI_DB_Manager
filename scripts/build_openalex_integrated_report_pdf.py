#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path


ROOT = Path("/home/kimyoungjin06/Desktop/Workspace/1.1.1.KISTI_DB_Manager")
OUT_DIR = ROOT / "output" / "pdf" / "openalex_20260330_integrated_report"


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _du_h(path: Path) -> str:
    known = {
        "/home/kimyoungjin06/Desktop/Disk/Raid/data/OpenAlex/parquet_exports/openalex_works_20260225_raw_20260331_031932": "938G",
        "/home/kimyoungjin06/Desktop/Disk/Raid/data/OpenAlex/parquet_exports/openalex_works_20260330_delta_20260407_212041": "105G",
        "/home/kimyoungjin06/Desktop/Disk/Raid/data/OpenAlex/parquet_exports/openalex_works_20260330_repairreplay_20260410_190630": "921G",
        "/home/kimyoungjin06/Desktop/HDD/Data/OpenAlex/reconstructed_abstract/openalex_works_abstract_reconstruct_20260330_20260412_231728": "94G",
    }
    if str(path) in known:
        return known[str(path)]
    out = subprocess.check_output(["du", "-sh", str(path)], text=True)
    return out.split()[0]


def _latex_escape(value: object) -> str:
    text = "" if value is None else str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def _fmt_int(value: int) -> str:
    return f"{int(value):,}"


def _short_work_id(value: object) -> str:
    text = "" if value is None else str(value)
    return text.rsplit("/", 1)[-1]


def _short_date(value: object) -> str:
    text = "" if value is None else str(value)
    return text[:10]


def _sample_rows(samples: list[dict], keys: list[str]) -> list[str]:
    rows: list[str] = []
    for item in samples:
        values = [_latex_escape(item.get(key, "")) for key in keys]
        rows.append(" & ".join(values) + r" \\")
    if not rows:
        rows.append(r"\multicolumn{4}{l}{No samples} \\")
    return rows


def _line(text: str) -> str:
    return text + "\n"


def main() -> int:
    change = _read_json(ROOT / "runs/openalex_works_20260330_change_report_20260414_120125/change_report.json")
    base_run = _read_json(ROOT / "runs/openalex_works_20260225_parquet_safe_run_20260331_031932/run_report.json")
    delta_run = _read_json(ROOT / "runs/openalex_works_20260330_delta_run_20260407_212041/run_report.json.progress.json")
    final_status = _read_json(ROOT / "runs/openalex_works_20260330_repairreplay_20260410_190630/final_status.json")
    dedupmain = _read_json(ROOT / "runs/openalex_works_20260330_dedupmain_20260410_134927/report.json")
    abstract_progress = _read_json(
        Path("/home/kimyoungjin06/Desktop/HDD/Data/OpenAlex/reconstructed_abstract/openalex_works_abstract_reconstruct_20260330_20260412_231728/progress.json")
    )

    base_root = Path(base_run["artifacts"]["persist_parquet_dir"])
    delta_root = Path("/home/kimyoungjin06/Desktop/Disk/Raid/data/OpenAlex/parquet_exports/openalex_works_20260330_delta_20260407_212041")
    final_root = Path(final_status["final_snapshot_root"])
    abstract_root = Path(abstract_progress["data_dir"]).parent

    base_size = _du_h(base_root)
    delta_size = _du_h(delta_root)
    final_size = _du_h(final_root)
    abstract_size = _du_h(abstract_root)
    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    title_changed_rows = _sample_rows(
        [
            {
                "id": _short_work_id(row["id"]),
                "base_title": str(row["base_title"])[:58],
                "final_title": str(row["final_title"])[:58],
                "changed": "Y",
            }
            for row in change["samples"]["title_changed"][:5]
        ],
        ["id", "base_title", "final_title", "changed"],
    )
    new_id_rows = _sample_rows(
        [
            {
                "id": _short_work_id(row["id"]),
                "title": str(row["title"])[:64],
                "publication_year": row["publication_year"],
                "updated_date": _short_date(row["updated_date"]),
            }
            for row in change["samples"]["new_ids"][:5]
        ],
        ["id", "title", "publication_year", "updated_date"],
    )

    lines: list[str] = []
    lines += [
        _line(r"\documentclass[11pt,a4paper]{article}"),
        _line(r"\usepackage{geometry}"),
        _line(r"\geometry{margin=22mm}"),
        _line(r"\usepackage{fontspec}"),
        _line(r"\setmainfont{Noto Serif CJK KR}"),
        _line(r"\setsansfont{Noto Sans CJK KR}"),
        _line(r"\setmonofont{Noto Sans Mono CJK KR}"),
        _line(r"\usepackage{array}"),
        _line(r"\usepackage{booktabs}"),
        _line(r"\usepackage{longtable}"),
        _line(r"\usepackage{tabularx}"),
        _line(r"\usepackage{xcolor}"),
        _line(r"\usepackage{hyperref}"),
        _line(r"\usepackage{fancyhdr}"),
        _line(r"\usepackage{enumitem}"),
        _line(r"\usepackage{titlesec}"),
        _line(r"\usepackage{lastpage}"),
        _line(r"\usepackage{url}"),
        _line(r"\hypersetup{colorlinks=true,linkcolor=blue,urlcolor=blue}"),
        _line(r"\pagestyle{fancy}"),
        _line(r"\fancyhf{}"),
        _line(r"\fancyhead[L]{OpenAlex 20260330 통합 보고서}"),
        _line(r"\fancyhead[R]{\thepage/\pageref{LastPage}}"),
        _line(r"\fancyfoot[L]{KISTI DB Manager}"),
        _line(r"\titleformat{\section}{\large\bfseries}{\thesection.}{0.6em}{}"),
        _line(r"\titleformat{\subsection}{\normalsize\bfseries}{\thesubsection.}{0.6em}{}"),
        _line(r"\setlength{\parindent}{0pt}"),
        _line(r"\setlength{\parskip}{0.4em}"),
        _line(r"\begin{document}"),
        _line(r"\begin{titlepage}"),
        _line(r"\centering"),
        _line(r"{\Huge OpenAlex 20260330 통합 보고서\par}"),
        _line(r"\vspace{0.8cm}"),
        _line(r"{\Large 이전 버전 / 증분 / 최종 버전 비교\par}"),
        _line(r"\vspace{1.5cm}"),
        _line(r"\begin{tabular}{ll}"),
        _line(f"기준 시각 & {_latex_escape(generated_at)} \\\\"),
        _line(r"이전 버전 & 20260225 full snapshot \\"),
        _line(r"증분 버전 & 20260226-20260330 delta snapshot \\"),
        _line(r"최종 버전 & 20260330 repaired final snapshot \\"),
        _line(r"\end{tabular}"),
        _line(r"\vfill"),
        _line(r"{\large Repository: \texttt{1.1.1.KISTI\_DB\_Manager}\par}"),
        _line(r"\end{titlepage}"),
        _line(r"\tableofcontents"),
        _line(r"\newpage"),
        _line(r"\section{핵심 요약}"),
        _line(r"\begin{itemize}[leftmargin=1.5em]"),
        _line(rf"\item 이전 버전은 20260225 기준 full snapshot이며, source records는 {_fmt_int(base_run['stats']['records_read'])}건이다."),
        _line(rf"\item 증분 버전은 20260226-20260330 범위의 delta로, distinct changed work id는 {_fmt_int(change['profile']['distinct_delta_ids'])}건이다."),
        _line(rf"\item 최종 버전은 증분 반영과 dedup/replay repair까지 완료된 canonical snapshot이며, main row와 distinct id는 모두 {_fmt_int(final_status['main_table']['rows'])}건이다."),
        _line(rf"\item work level 삭제는 현재 merge 설계상 없고, 대신 abstract 제거 {_fmt_int(change['abstract_counts']['abstract_removed'])}건 같은 field-level removal이 있다."),
        _line(r"\end{itemize}"),
        _line(r"\section{버전별 개요}"),
        _line(r"\begin{longtable}{p{3.1cm}p{2.2cm}p{2.5cm}p{2.6cm}p{4.3cm}}"),
        _line(r"\toprule"),
        _line(r"버전 & 시점 & 규모 & 핵심 건수 & 비고 \\"),
        _line(r"\midrule"),
        _line(rf"이전 버전 & 20260225 & {base_size} & source records {_fmt_int(base_run['stats']['records_read'])} & full snapshot parse 결과 \\"),
        _line(rf"증분 버전 & 20260330 & {delta_size} & delta ids {_fmt_int(change['profile']['distinct_delta_ids'])} & 신규 {_fmt_int(change['profile']['new_ids'])}, 기존 변경 {_fmt_int(change['profile']['overlap_existing_ids'])} \\"),
        _line(rf"최종 버전 & 20260330 final & {final_size} & main rows {_fmt_int(final_status['main_table']['rows'])} & dedup main + replay repair 반영 \\"),
        _line(rf"plain abstract & 20260330 final & {abstract_size} & rows {_fmt_int(abstract_progress['rows_written'])} & \texttt{{oaid\_w, has\_abstract, abstract}} 형태로 복원 \\"),
        _line(r"\bottomrule"),
        _line(r"\end{longtable}"),
        _line(r"\section{이전 버전(20260225 full snapshot)}"),
        _line(r"\begin{itemize}[leftmargin=1.5em]"),
        _line(rf"\item 파싱 기간: {_latex_escape(base_run['started_at'])} - {_latex_escape(base_run['finished_at'])}"),
        _line(rf"\item source records read: {_fmt_int(base_run['stats']['records_read'])}"),
        _line(rf"\item parquet files persisted: {_fmt_int(base_run['stats']['parquet_files_persisted'])}"),
        _line(rf"\item parquet rows emitted across all tables: {_fmt_int(base_run['stats']['parquet_rows_emitted'])}"),
        _line(r"\item later audit에서 main replay duplicate 879건이 발견되었고, 이는 최종본 생성 시 제거되었다."),
        _line(r"\end{itemize}"),
        _line(r"\section{증분 버전(20260226-20260330 delta)}"),
        _line(r"\begin{itemize}[leftmargin=1.5em]"),
        _line(rf"\item delta source records read: {_fmt_int(delta_run['stats']['records_read'])}"),
        _line(rf"\item parquet files persisted: {_fmt_int(delta_run['stats']['parquet_files_persisted'])}"),
        _line(rf"\item distinct changed work ids: {_fmt_int(change['profile']['distinct_delta_ids'])}"),
        _line(rf"\item 기존 work에 대한 업데이트: {_fmt_int(change['profile']['overlap_existing_ids'])}"),
        _line(rf"\item 신규 work ids: {_fmt_int(change['profile']['new_ids'])}"),
        _line(r"\end{itemize}"),
        _line(r"\subsection{주요 필드 변경}"),
        _line(r"\begin{tabularx}{\textwidth}{l>{\raggedleft\arraybackslash}X}"),
        _line(r"\toprule"),
        _line(r"항목 & 변경 건수 \\"),
        _line(r"\midrule"),
        _line(rf"title/display\_name & {_fmt_int(change['main_field_counts']['title_changed'])} \\"),
        _line(rf"publication\_date & {_fmt_int(change['main_field_counts']['publication_date_changed'])} \\"),
        _line(rf"publication\_year & {_fmt_int(change['main_field_counts']['publication_year_changed'])} \\"),
        _line(rf"type & {_fmt_int(change['main_field_counts']['type_changed'])} \\"),
        _line(rf"authors\_count & {_fmt_int(change['main_field_counts']['authors_count_changed'])} \\"),
        _line(rf"institutions\_distinct\_count & {_fmt_int(change['main_field_counts']['institutions_distinct_count_changed'])} \\"),
        _line(rf"countries\_distinct\_count & {_fmt_int(change['main_field_counts']['countries_distinct_count_changed'])} \\"),
        _line(rf"cited\_by\_count & {_fmt_int(change['main_field_counts']['cited_by_count_changed'])} \\"),
        _line(rf"fwci & {_fmt_int(change['main_field_counts']['fwci_changed'])} \\"),
        _line(rf"has\_fulltext & {_fmt_int(change['main_field_counts']['has_fulltext_changed'])} \\"),
        _line(r"\bottomrule"),
        _line(r"\end{tabularx}"),
        _line(r"\subsection{abstract / authorship 변화}"),
        _line(r"\begin{tabularx}{\textwidth}{l>{\raggedleft\arraybackslash}X}"),
        _line(r"\toprule"),
        _line(r"항목 & 건수 \\"),
        _line(r"\midrule"),
        _line(rf"abstract added & {_fmt_int(change['abstract_counts']['abstract_added'])} \\"),
        _line(rf"abstract removed & {_fmt_int(change['abstract_counts']['abstract_removed'])} \\"),
        _line(rf"abstract payload changed & {_fmt_int(change['abstract_counts']['abstract_payload_changed'])} \\"),
        _line(rf"authorship rowcount changed & {_fmt_int(change['authorship_counts']['authorship_rowcount_changed'])} \\"),
        _line(rf"authorship added from zero & {_fmt_int(change['authorship_counts']['authorship_added_from_zero'])} \\"),
        _line(rf"authorship removed to zero & {_fmt_int(change['authorship_counts']['authorship_removed_to_zero'])} \\"),
        _line(r"\bottomrule"),
        _line(r"\end{tabularx}"),
        _line(r"\section{최종 버전(20260330 repaired final snapshot)}"),
        _line(r"\begin{itemize}[leftmargin=1.5em]"),
        _line(rf"\item 최종 스냅샷 아티팩트: \texttt{{{_latex_escape(final_root.name)}}}"),
        _line(r"\item 저장 위치: RAID / data/OpenAlex/parquet\_exports"),
        _line(rf"\item main rows: {_fmt_int(final_status['main_table']['rows'])}"),
        _line(rf"\item distinct ids: {_fmt_int(final_status['main_table']['distinct_ids'])}"),
        _line(rf"\item duplicate id rows: {_fmt_int(final_status['main_table']['duplicate_id_rows'])}"),
        _line(rf"\item replay repair touched tables: {_fmt_int(final_status['replay_repair']['touched_tables'])}"),
        _line(rf"\item replay repair rows removed: {_fmt_int(final_status['replay_repair']['rows_removed'])}"),
        _line(rf"\item linked untouched tables: {_fmt_int(final_status['replay_repair']['linked_tables'])}"),
        _line(r"\end{itemize}"),
        _line(r"\subsection{dedup 및 validation}"),
        _line(r"\begin{tabularx}{\textwidth}{l>{\raggedleft\arraybackslash}X}"),
        _line(r"\toprule"),
        _line(r"검증 항목 & 값 \\"),
        _line(r"\midrule"),
        _line(rf"main duplicate rows removed & {_fmt_int(dedupmain['steps']['validate_rewrite']['rows_removed'])} \\"),
        _line(rf"repaired abstract duplicate rows removed & {_fmt_int(final_status['key_followups']['abstract']['dedup_affected_rows'])} \\"),
        _line(rf"repaired authorships duplicate rows removed & {_fmt_int(final_status['key_followups']['authorships']['dedup_affected_rows'])} \\"),
        _line(rf"repaired concepts duplicate rows removed & {_fmt_int(final_status['key_followups']['concepts']['dedup_affected_rows'])} \\"),
        _line(r"orphan rows in authorships/concepts/locations/referenced\_works/topics & 0 / 0 / 0 / 0 / 0 \\"),
        _line(r"\bottomrule"),
        _line(r"\end{tabularx}"),
        _line(r"\section{작업용 파생 산출물}"),
        _line(r"\begin{itemize}[leftmargin=1.5em]"),
        _line(rf"\item plain abstract 아티팩트: \texttt{{{_latex_escape(abstract_root.name)}}}"),
        _line(r"\item 저장 위치: HDD / Data/OpenAlex/reconstructed\_abstract"),
        _line(rf"\item reconstructed rows: {_fmt_int(abstract_progress['rows_written'])}"),
        _line(rf"\item has\_abstract = Y: {_fmt_int(abstract_progress['has_abstract_y'])}"),
        _line(rf"\item has\_abstract = N: {_fmt_int(abstract_progress['has_abstract_n'])}"),
        _line(r"\item 현재 작업용 통합 파일은 아직 생성 전이며, main/title + plain abstract + institution aggregation을 결합한 slim dataset을 별도 생성하는 것이 권장된다."),
        _line(r"\end{itemize}"),
        _line(r"\section{변경 샘플}"),
        _line(r"\subsection{신규 work 예시}"),
        _line(r"{\small"),
        _line(r"\begin{longtable}{p{2.4cm}p{9.0cm}p{1.3cm}p{2.2cm}}"),
        _line(r"\toprule"),
        _line(r"work id & title & year & updated \\"),
        _line(r"\midrule"),
    ]
    lines += [_line(row) for row in new_id_rows]
    lines += [
        _line(r"\bottomrule"),
        _line(r"\end{longtable}"),
        _line(r"}"),
        _line(r"\subsection{title 변경 예시}"),
        _line(r"{\small"),
        _line(r"\begin{longtable}{p{2.2cm}p{5.7cm}p{5.7cm}p{0.8cm}}"),
        _line(r"\toprule"),
        _line(r"work id & base title & final title & changed \\"),
        _line(r"\midrule"),
    ]
    lines += [_line(row) for row in title_changed_rows]
    lines += [
        _line(r"\bottomrule"),
        _line(r"\end{longtable}"),
        _line(r"}"),
        _line(r"\section{결론}"),
        _line(r"\begin{itemize}[leftmargin=1.5em]"),
        _line(r"\item 20260225 full snapshot에 20260330 delta를 반영한 최종 canonical snapshot은 준비 완료 상태다."),
        _line(rf"\item 증분 반영 결과, 신규 work {_fmt_int(change['profile']['new_ids'])}건과 기존 work 업데이트 {_fmt_int(change['profile']['overlap_existing_ids'])}건이 확인되었다."),
        _line(r"\item 최종 snapshot은 main unique id 기준으로 정제 완료되었고, 핵심 child table orphan은 발견되지 않았다."),
        _line(r"\item 다음 단계는 작업용 전달본(title + abstract + institution aggregation) 생성과 DB 적재 전략 재조정이다."),
        _line(r"\end{itemize}"),
        _line(r"\end{document}"),
    ]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tex_path = OUT_DIR / "openalex_20260330_integrated_report.tex"
    tex_path.write_text("".join(lines), encoding="utf-8")

    for _ in range(2):
        subprocess.run(
            ["/home/kimyoungjin06/.local/bin/xelatex", "-interaction=nonstopmode", "-halt-on-error", tex_path.name],
            cwd=OUT_DIR,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    pdf_path = OUT_DIR / "openalex_20260330_integrated_report.pdf"
    render_dir = OUT_DIR / "rendered_pages"
    render_dir.mkdir(exist_ok=True)
    subprocess.run(
        ["/usr/bin/pdftoppm", "-png", str(pdf_path), str(render_dir / "page")],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
