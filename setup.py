from setuptools import setup, find_namespace_packages

setup(
    name="mr_supabase",
    version="0.1.0",
    description="MindRoot Supabase Database Integration",
    author="MindRoot",
    author_email="info@mindroot.ai",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        "mr_supabase": [
            "templates/*.jinja2",
            "static/js/*.js",
            "inject/*.jinja2",
            "override/*.jinja2"
        ],
    },
    install_requires=[
        "supabase>=1.0.0",
        "python-dotenv>=0.19.0",
    ],
    python_requires=">=3.8",
)
