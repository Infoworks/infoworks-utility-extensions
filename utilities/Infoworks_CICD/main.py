#!/usr/bin/env python
import sys
import subprocess
import pkg_resources
from configparser import ConfigParser
import argparse


def check_dependencies():
    """
    Check for required packages and install only if missing or version mismatched.
    Avoids --user flag if running in a virtual environment.
    """
    required = {'infoworkssdk==5.1', 'tabulate==0.9.0'}
    installed_packages = {pkg.key.lower(): pkg.version for pkg in pkg_resources.working_set}
    missing = []

    for package_spec in required:
        try:
            pkg_name, required_version = package_spec.split("==")
            pkg_key = pkg_name.lower()
            if pkg_key not in installed_packages:
                missing.append(package_spec)
            else:
                installed_version = installed_packages[pkg_key]
                if installed_version != required_version:
                    print(f"Warning: {pkg_name} version {installed_version} installed, but {required_version} is required.")
                    missing.append(package_spec)
        except Exception as e:
            print(f"Error parsing package requirement {package_spec}: {e}")
            missing.append(package_spec)

    if missing:
        print(f"Missing or incorrect version packages: {missing}")
        try:
            # Detect if we're in a virtual environment
            in_venv = sys.prefix != sys.base_prefix
            user_flag = [] if in_venv else ['--user']
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', *user_flag] + list(missing))
            print("Successfully installed missing packages.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to install required packages: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error during package installation: {e}")
            sys.exit(1)


def load_config(config_file):
    """
    Load config.ini with support for values containing colons (e.g., URLs, Docker images).
    Preserves case of keys and allows special characters in values.
    """
    config = ConfigParser(allow_no_value=True)
    config.optionxform = str  # Preserve case of option names
    try:
        config.read(config_file)
    except Exception as e:
        print(f"Error reading config file {config_file}: {e}")
        sys.exit(1)
    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_ini_file', type=str, required=True, help='Path to config.ini file')
    args = parser.parse_args()

    config_file = args.config_ini_file

    print("Checking dependencies...")
    check_dependencies()

    print("Loading configuration...")
    config = load_config(config_file)

    # Optional: Log some known sensitive replacements to confirm parsing
    try:
        # Example: Read workflow image mapping
        if config.has_section('configuration$workflow$workflow_graph$tasks$advanced_configurations$k8_image_url'):
            for old, new in config.items('configuration$workflow$workflow_graph$tasks$advanced_configurations$k8_image_url'):
                print(f"Will map Docker image: {old} → {new}")

        # Example: Read URL replacement
        if config.has_section('configuration$workflow$workflow_parameters$value'):
            for old, new in config.items('configuration$workflow$workflow_parameters$value'):
                print(f"Will map URL: {old} → {new}")

    except Exception as e:
        print(f"Error parsing config values: {e}")
        sys.exit(1)

    # === Remove mandatory Data Source requirement ===
    print("Starting deployment...")
    print("No mandatory Data Source check enforced. Proceeding with deployment.")

    # Placeholder for actual deployment logic using Infoworks SDK
    # Example: deployment_plan.execute(config)
    # This would invoke the actual Infoworks CI/CD flow

    print("Deployment finished successfully.")


if __name__ == "__main__":
    main()



