# E15: GDTM, a pipeline whose real shape is mixed

GDTM's all-modalities early-fusion tracker (UCLA, github.com/nesl/GDTM-tracking),
ported in `examples/gdtm/` with its released checkpoint and a recorded
segment of its sample test data. Three sensor nodes, each with a ZED camera,
a RealSense depth camera, a TI mmWave radar and a ReSpeaker array; 29 tasks.

Stage kinds follow the real deployment, not the testbed:

- **Containers:** sensor capture (vendor SDKs; here a replay), plus the backbones, adapters and fusion head. The model stages run the repo's own code and checkpoint on its pinned stack (mmcv-full 1.7, the repo's mmdetection 2.x and mmclassification forks, torch 1.12), which cannot share an image with a current-torch runner.
- **Functions:** preprocessing (JPEG decode, normalize, range-Doppler scaling, spectrogram) and the Kalman tracker.

Setup: sources pinned to anrg-4, anrg-5, anrg-9; built-in HEFT; 30 frames
(2 s at 15 fps) from frame 600 of the test split; default clocks; clean
network. `gdtmrun.py <mode> <frames> <reference.npz>` runs one variant,
prints the stage timeline with pod lifecycle, and checks the detections and
the track against the unsplit reference.

## Fidelity

`reference.py` runs the unsplit released model through the repo's own data
path, test loop and evaluation on CPU, then the split model stages on the
same inputs. `check_glue.py` runs the function stages on the current stack.

| boundary | max difference |
|---|---|
| 12 embeddings, detections, covariances (split model vs unsplit) | 0 (bit-exact) |
| covariance calibration (ours vs repo's) | 0 |
| camera, depth, radar preprocessing (OpenCV 4.x current vs repo's mmcv path) | 0 (bit-exact) |
| audio spectrogram (current torch vs torchaudio 0.12) | 2.2e-8 |
| Kalman track | 1.9e-6 cm |
| whole Wayline run vs reference: detections / track | 3.8e-5 cm / 1.9e-5 cm |

Track error against OptiTrack ground truth over the window: 5.31 cm mean,
2.97 cm over the second half (the filter starts at the origin).

## Results (2026-09-29, three runs each, after the fixes below)

| variant | makespan (s) | median |
|---|---|---|
| all pods | 32.9, 34.6, 34.5 | 34.5 |
| mixed (the pipeline's real shape) | 27.9, 29.5, 29.2 | 29.2 |
| all warm (glue runner + a warm runner on the pinned model stack) | 15.9, 17.4, 17.6 | 17.4 |

Every run's detections and track match the unsplit reference (at most
3.8e-5 cm and 1.9e-5 cm), every task was dispatched exactly once (135 pod
launches for 135 pod tasks), and every run was marked Succeeded within
2.7 s of its last task.

Where the time goes in the mixed run: the critical path is a sensor pod
(about 1.3 s to start, 1.3 s of replay), the preprocessing functions
(0.2 s), the ZED embed container (1.4 to 3.3 s from pod creation to task
start, about 2.5 s importing the OpenMMLab stack and loading its weights,
10 to 12 s of compute on 2 threads), the fusion container (the same
start-up, 2.1 s of compute) and the track function (milliseconds). All-warm
removes both container start-ups from the critical path, at the price of
keeping the pinned model stack resident on every node (560 MiB each, below).
All-pods adds the start-up to the sensor, preprocessing and track stages
too.

The plan did not see this: built-in HEFT predicted 16.2 s for the mixed
run, because the template's runtimes are compute only and the planner has
no notion that a container stage pays seconds to start while a warm call
does not.

## Found and fixed on the way

1. **A finished pod held its CPU until the kubelet reported it.** The
   controller's shared CPU admission counted a pod until its phase became
   Succeeded. On a node whose kubelet was busy (pulling the 1 GB model
   image) that report lagged by about 20 s, so a waiting 2-CPU embed stage
   sat idle behind a source pod that had finished long before. The first
   mixed run took 61.8 s. Now, when a task does not fit, the controller
   asks the node's data agent which counted pods have already reported
   ComputeDone and stops counting them (keyed by pod UID, so a retry counts
   again), and logs every deferral with its breakdown. The same pipeline
   then ran in 29.0 s. `capacity.go`, test in `capacity_test.go`.
2. **Warm calls deadlocked after a preload that ran parallel kernels.** The
   runner imports `--preload` modules in the parent and forks from it.
   Preloading GDTM's model ran torch's parallel kernels (loading weights),
   which started a libgomp thread pool that forked calls inherit without
   its threads: the first parallel kernel in a call waited forever (the
   ZED and depth embeds hung; the small radar and audio ones, below the
   parallel grain, finished). Reproduced on torch 1.12 with a two-level
   fork. The runner now preloads with one OpenMP/BLAS thread and gives each
   call as many threads as CPUs it requested. FOCAL never hit this because
   its tensors are small. `sdk/python/wl/runner.py`, test in
   `sdk/python/tests/test_runner_threads.py`.
3. **Run status went backwards, and completion came late.** Every pod
   event, warm-call completion and 500 ms sweep starts a dispatch pass,
   concurrently. Each pass also rebuilt `status.tasks` from its own pod
   snapshot, querying the data agents one task at a time, wrote the whole
   list, then checked completion. With 29 tasks a pass took seconds and
   passes finished out of order: the task list flapped between 6 and 29
   entries, tasks went from Succeeded back to Pending, a run read right
   after it succeeded could miss its last tasks (two of three all-pods
   repetitions lost their result this way), and a run whose last task ended
   at 37 s was marked Succeeded at 51 to 66 s. Status and completion now run
   in a per-run worker, one pass at a time: requests during a pass coalesce
   into one more pass that snapshots when it starts, so status never goes
   backwards; per-task agent queries run concurrently; success is recorded
   once per run. The same run is now marked Succeeded within 1 s of its
   last task. `statusworker.go`, test in `statusworker_test.go`.
4. Torch 1.12's `libtorch_cpu.so` needs an executable stack, which glibc
   2.41 refuses; the model image uses the bookworm base.

## Resident cost of keeping each stack warm (per node, idle)

| runner | memory |
|---|---|
| gdtm-model (the pinned stack, all weights preloaded) | 560 MiB |
| gdtm (glue) | 195 MiB |
| focal | 311 MiB |
