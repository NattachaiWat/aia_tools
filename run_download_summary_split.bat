@echo off
SET CONNECTION_STRING=
python download_analysis_split.py \
  --save_path . \
  --project_code AIA008 \
  --commit_id 84c1166e5e23887e34191f1a2e96bfe7e575f978,30acb402fa0b0d1c670d394a186dd7f81fd6252e \
  --excel_input output_excel_AIA008_20230321_191130_benchmark_only.xlsx \
  --start_date 20230320-0000 \
  --stop_date 20230322-0000