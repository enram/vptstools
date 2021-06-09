from setuptools import setup, find_packages

setup(
    name='vptstools',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    python_requires=">3.7.0",
    install_requires=[
        'Click==8.0.1',
        'odimh5==0.1.0'
    ],
    entry_points={
        'console_scripts': [
            'vph5_to_vpts = vptstools.scripts.vph5_to_vpts:cli',
        ],
    },
)