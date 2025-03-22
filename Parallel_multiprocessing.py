from mp_shared_array import MemorySharedNumpyArray
import numpy as np

image_array_xy = MemorySharedNumpyArray(shape=(500, 500), dtype=np.int16, sampling=1)
