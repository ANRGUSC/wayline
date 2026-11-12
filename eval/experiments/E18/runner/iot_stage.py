"""Warm-runner entry for the iot tasks: run the task script of this call's
stage, unchanged (the runner applies the task's environment first)."""
import os
import runpy

STAGE = {"capture": "capture", "preprocess": "preprocess", "infer": "infer",
         "fuse": "fuse", "generate": "report"}
role = STAGE[os.environ["WL_TASK_NAME"].split("-")[0]]
runpy.run_path(f"/app/tasks/{role}/task.py", run_name="__main__")
