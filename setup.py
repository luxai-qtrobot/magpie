import os
from setuptools import setup, find_packages


current_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_dir, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="magpie",
    version="0.1.0",
    description="MAGPIE: Modular AI General Purpose Integration Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Ali PAikan",
    author_email="ali.paikan@gmail.com",
    url="https://github.com/luxai-qtrobot/magpie",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "python-ulid>=1.1.0"
        "msgpack>=1.1.1"
        "pyzmq>=27.1.0"    
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    python_requires=">=3.7",
    license="MIT",
    keywords="communication integration AI",
    project_urls={
        "Documentation": "https://github.com/luxai-qtrobot/magpie#readme",
        "Source": "https://github.com/luxai-qtrobot/magpie",
        "Tracker": "https://github.com/luxai-qtrobot/magpie/issues",
    },
)
