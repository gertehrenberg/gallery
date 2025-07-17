from setuptools import setup, find_packages

setup(
    name="gallery-lib",
    version="0.1",
    packages=["app", "app.routes", "app.scores", "app.services", "app.templates", "app.utils"],
    python_requires=">=3.12",
    install_requires=[
        "boto3",
        "click",
        "geopy",
        "jinja2",
        "networkx",
        "numpy",
        "opencv-python",
        "pandas",
        "paramiko",
        "pillow",
        "protobuf",
        "pyparsing",
        "pytz",
        "pyyaml",
        "requests",
        "scipy",
    ]
)