import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from random import random, uniform, choice, randint
from scipy.ndimage import rotate, gaussian_filter
from colorsys import hls_to_rgb
import tempfile


class ImageGenerationGateway:
    def random_color(self, base_hue: float, hue_range: int = 60) -> str:
        hue = (base_hue + uniform(-hue_range / 360, hue_range / 360)) % 1.0
        saturation = uniform(0.4, 0.8)
        lightness = uniform(0.4, 0.7)
        r, g, b = hls_to_rgb(hue, lightness, saturation)

        return "#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255))

    def generate_colormap(self, num_colors: int = 10) -> ListedColormap:
        base_hue = random()
        colors = [self.random_color(base_hue=base_hue) for _ in range(num_colors)]

        return ListedColormap(colors)

    def generate_background(self, size: int, dominant_shape: str) -> np.ndarray:
        background = np.zeros((size, size))

        for _ in range(500):
            x_center, y_center = randint(0, size), randint(0, size)
            intensity = uniform(0.1, 1.0)
            pattern_type = (
                dominant_shape
                if random() < 0.7
                else choice(["circle", "stripe", "blob"])
            )
            rotation = randint(0, 360)
            pattern_size = randint(20, 200)

            if pattern_type == "circle":
                y, x = np.ogrid[
                    -x_center : size - x_center, -y_center : size - y_center
                ]
                mask = x**2 + y**2 <= pattern_size**2
            elif pattern_type == "stripe":
                stripe_width = randint(10, 50)
                mask = (
                    np.abs(y_center - np.arange(size)[:, None]) % (2 * stripe_width)
                    < stripe_width
                )
                mask = rotate(mask, angle=rotation, reshape=False)
            elif pattern_type == "blob":
                blob = np.random.random((pattern_size, pattern_size))
                blob = gaussian_filter(blob, sigma=uniform(1, 5))
                mask = np.zeros_like(background)
                x_start = max(0, x_center - blob.shape[0] // 2)
                y_start = max(0, y_center - blob.shape[1] // 2)
                x_end = min(background.shape[0], x_start + blob.shape[0])
                y_end = min(background.shape[1], y_start + blob.shape[1])
                mask[x_start:x_end, y_start:y_end] = blob[
                    : x_end - x_start, : y_end - y_start
                ]
            else:
                mask = np.zeros_like(background)

            background += mask * intensity

        return background % 1

    def warp_image(self, image: np.ndarray, intensity: float) -> np.ndarray:
        rows, cols = image.shape
        x = np.linspace(-np.pi, np.pi, cols)
        y = np.linspace(-np.pi, np.pi, rows)
        x, y = np.meshgrid(x, y)
        func_x = choice([np.sin, np.cos, np.tan])
        func_y = choice([np.sin, np.cos, np.tan])
        warp_x = 1 + intensity * func_x(x)
        warp_y = 1 + intensity * func_y(y)
        warped_image = image * warp_x * warp_y

        return np.clip(warped_image, 0, 1)

    def generate_random_image(self, size: int = 2048, num_colors: int = 10) -> str:
        cmap = self.generate_colormap(num_colors)
        dominant_shape = choice(["circle", "stripe", "blob"])
        background = self.generate_background(size, dominant_shape)
        warp_intensity = choice([0, 0.5, 1.0])

        if warp_intensity > 0:
            background = self.warp_image(background, warp_intensity)

        background = background % num_colors

        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
            plt.imsave(temp_file.name, background, cmap=cmap)
            temp_file_path = temp_file.name

        return temp_file_path
