"""The glue of GDTM's early-fusion tracker, as plain functions on a current
stack (numpy, OpenCV, torch): sensor replay, the dataset's per-modality
preprocessing, and the Kalman tracker.

The preprocessing re-expresses the repo's mmdet pipelines
(configs/_base_/datasets/ucla_1car_early_fusion.py) with the same OpenCV and
torch calls mmcv makes, so it needs no mmcv; the tracker is the repo's own
TorchMultiObsKalmanFilter (src/resource_constrained_tracking/tracker.py).
reference.py checks every stage against the repo's data path.
"""
import os

import numpy as np

SEGMENT = os.environ.get("GDTM_SEGMENT", "/opt/gdtm-segment")
FILES = {"zed_camera_left": "zed.hdf5", "realsense_camera_depth": "realsense.hdf5",
         "range_doppler": "mmwave.hdf5", "mic_waveform": "respeaker.hdf5"}
JPEG = ("zed_camera_left", "realsense_camera_depth")
IMG_MEAN = np.array([123.675, 116.28, 103.53], np.float32)
IMG_STD = np.array([58.395, 57.12, 57.375], np.float32)


def read(node, mod, start, frames):
    """One sensor's recording on one node, frames [start, start+frames) of the
    segment. JPEG streams come back as a list of encoded frames."""
    import h5py
    with h5py.File(f"{SEGMENT}/{node}/{FILES[mod]}") as h:
        keys = sorted(h.keys(), key=int)[start:start + frames]
        vals = [h[k][node][mod][:] for k in keys]
    return vals if mod in JPEG else np.stack(vals)


# mmcv.imrescale / imresize / imnormalize, cv2 backend, bilinear.
def _imrescale(img, scale):
    h, w = img.shape[:2]
    s = min(max(scale) / max(h, w), min(scale) / min(h, w))
    return _imresize(img, (int(w * s + 0.5), int(h * s + 0.5)))


def _imresize(img, size):
    import cv2
    return cv2.resize(img, size, interpolation=cv2.INTER_LINEAR)


def _imnormalize(img, mean, std, to_rgb):
    import cv2
    img = img.copy().astype(np.float32)
    mean = np.float64(mean.reshape(1, -1))
    stdinv = 1 / np.float64(std.reshape(1, -1))
    if to_rgb:
        cv2.cvtColor(img, cv2.COLOR_BGR2RGB, img)
    cv2.subtract(img, mean, img)
    cv2.multiply(img, stdinv, img)
    return img


def _chw(img):  # DefaultFormatBundle
    if img.ndim < 3:
        img = np.expand_dims(img, -1)
    return np.ascontiguousarray(img.transpose(2, 0, 1))


def prep_image(codes):
    """img_pipeline: DecodeJPEG, LoadFromNumpyArray, Resize((270, 480),
    keep_ratio), RandomFlip(0), Normalize(ImageNet, to_rgb)."""
    import cv2
    out = []
    for code in codes:
        img = np.nan_to_num(cv2.imdecode(code, 1), nan=0.0)
        img = _imrescale(img, (270, 480))
        out.append(_chw(_imnormalize(img, IMG_MEAN, IMG_STD, True)))
    return np.stack(out)


def prep_radar(x):
    """range_pipeline: LoadFromNumpyArray(float32, transpose), Resize((256, 16),
    keep_ratio), RandomFlip(0), Normalize(4353, 705, to_rgb)."""
    out = []
    for a in x:
        a = np.nan_to_num(a.astype(np.float32).T[:, :, np.newaxis], nan=0.0)
        a = _imrescale(a, (256, 16))
        out.append(_chw(_imnormalize(a, np.array([4353], np.float32), np.array([705], np.float32), True)))
    return np.stack(out)


def prep_audio(x):
    """audio_pipeline: LoadAudio(n_fft=75) (torchaudio's Spectrogram: hann
    window, hop 37, centered, reflect padding, power 2), Resize((32, 32)),
    RandomFlip(0)."""
    import torch
    window = torch.hann_window(75)
    out = []
    for a in x:
        t = torch.from_numpy(np.ascontiguousarray(a[:, 1:5])).unsqueeze(0).permute(0, 2, 1)
        spec = torch.stft(t.reshape(-1, t.shape[-1]), n_fft=75, hop_length=37, win_length=75, window=window,
                          center=True, pad_mode="reflect", normalized=False, onesided=True,
                          return_complex=True).abs().pow(2.0)
        spec = spec.reshape(t.shape[:-1] + spec.shape[-2:])
        sgram = spec.permute(0, 2, 3, 1).squeeze().numpy()
        out.append(_chw(_imresize(sgram, (32, 32))))
    return np.stack(out)


PREP = {"zed_camera_left": prep_image, "realsense_camera_depth": prep_image,
        "range_doppler": prep_radar, "mic_waveform": prep_audio}


def track(means, covs, a, b):
    """Calibrate the detections' covariances (a * S + b * I, the validation
    grid search's choice) and run the repo's Kalman filter over the window,
    as HDF5Dataset.calibrate_outputs and track_eval do. Returns tracked
    positions [frames, 2] and covariances [frames, 2, 2] (cm)."""
    import torch
    from tracker import TorchMultiObsKalmanFilter
    det_means = [torch.from_numpy(np.ascontiguousarray(m)).reshape(2, 1) for m in means]
    det_covs = [[a * torch.from_numpy(np.ascontiguousarray(c)) + b * torch.eye(2)] for c in covs]
    kf = TorchMultiObsKalmanFilter(dt=1, std_acc=1)
    with torch.no_grad():
        pos, cov = kf.forward(det_means, det_covs)
    return pos.t().numpy(), cov.permute(2, 0, 1).numpy()
