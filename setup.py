import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, find_packages


setup(
    name='rsem-pipeline',
    version='1.0',

    zip_safe=True,

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2.7',
    ],

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    # packages=setuptools.find_packages(exclude=['venv*', 'fq_gz_temp_stats', 'non-vc']),
    packages=find_packages(),

    # https://packaging.python.org/en/latest/technical.html#install-requires-vs-requirements-files
    install_requires=[
        'beautifulsoup4>=4.3.2',
        'Jinja2>=2.7.3',
        'paramiko>=1.14.0',
        'PyYAML==3.11',
        'ruffus>=2.4.1',
        'requests>=2.3.0',
    ],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    # entry_points={
    #     'console_scripts': [
    #         'sample=sample:main',
    #     ],
    # },


    # metadata for upload to PyPI
    author="Zhuyi Xue",
    author_email="zxue@bcgsc.ca",
    description="A pipeline for analyzing GEO samples using rsem",
    long_description="rsem as in http://deweylab.biostat.wisc.edu/rsem/README.html (more to be written)",
    license="GPLv3",
    keywords="GEO rsem pipeline",
    url="https://github.com/bcgsc/rsem_pipeline.git",

    include_package_data=True,
)

