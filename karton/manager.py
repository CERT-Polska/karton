#!/usr/bin/env python3
import click
import jinja2
import os
import sys
import json
import unittest
import subprocess
import requests
from shutil import copyfile

TEMPLATE_FOLDER = os.path.join(os.path.dirname(__file__), "templates")
CONFIG_PATH = os.path.expanduser("~/.kpm/config.json")


class ConfigLoadFailure(BaseException):
    """
    Config is non existent, broken or cannot be accessed
    """


class KubernetesNotEnabled(BaseException):
    """
    Kubernetes support disabled in ~/.kpm/config.json
    """


class KartonStatusAPI:
    def __init__(self):
        self.url = "https://karton-status.cert.pl/"

    def status(self, uid):
        r = requests.get(self.url + str(uid))
        r.raise_for_status()
        return r.json()


class Config(dict):
    @classmethod
    def from_json(cls, json_content):
        try:
            config = json.loads(json_content)
        except json.JSONDecodeError:
            raise ValueError("Invalid config")

        return cls.from_dict(config)

    @classmethod
    def from_dict(cls, dict_):
        return cls(dict_)

    @classmethod
    def from_path(cls, path):
        config = None
        with open(path, "r") as config_file:
            config = config_file.read()
        if not config:
            raise ConfigLoadFailure()
        return Config.from_json(config)

    @classmethod
    def from_default_path(cls):
        return cls.from_path(CONFIG_PATH)

    def save(self, path):
        with open(path, "w") as config_file:
            config_file.write(json.dumps(self))

    def __getattr__(self, item):
        return self[item]


@click.group()
def karton():
    """karton package manager"""
    if not os.path.exists(CONFIG_PATH):
        click.echo("This seems like the first run of the kpm")
        click.echo("Let's generate config in ~/.kpm/config.json")
        prefix = click.prompt("Docker registry prefix", default="dr.cert.pl/karton")
        kubernetes = click.confirm("Do you have access to karton-prod(with kubectl)?")

        config = {
            "dockerregistry_prefix": prefix,
            "use_kubernetes": kubernetes,
        }
        kpm_path = os.path.expanduser("~/.kpm")
        if not os.path.isdir(kpm_path):
            os.makedirs(kpm_path)
        c = Config(config)
        c.save(CONFIG_PATH)


# cancer.
@karton.command(
    "deploy",
    short_help="build and push karton image to repository",
    add_help_option=False,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@click.pass_context
def deploy(ctx):
    from deploy import main as deploy_main

    c = Config.from_default_path()
    if not c.use_kubernetes:
        raise KubernetesNotEnabled()
    # ???
    sys.argv = ["kpm deploy"] + ctx.args
    deploy_main()


def render(from_, to, render_vars):
    render_environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(TEMPLATE_FOLDER)
    )

    output_text = render_environment.get_template(from_).render(render_vars)

    with open(to, "w") as deploy_file:
        deploy_file.write(output_text)


@karton.command("status", short_help="get task's status in JSON format")
@click.argument("uid")
def status(uid):
    status_api = KartonStatusAPI()
    status_result = status_api.status(uid)
    print(status_result)


@karton.command("logs", short_help="get task's logs url")
@click.argument("uid")
def logs(uid):
    print(
        "https://buzz.cert.pl:8000/en-GB/app/search/karton_task_information?form.TASKUID={}&form.logthreshold=20".format(
            uid
        )
    )


@karton.command("test", short_help="run unit tests for karton")
@click.option("--test-dir", type=str, default="tests", help="Test cases directory")
@click.pass_context
def test(ctx, test_dir):
    # We need current dir in sys.path to import tested subsystem class
    sys.path.append(os.getcwd())
    suite = unittest.TestLoader().discover(test_dir)
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        ctx.abort()


@karton.command("init", short_help="initialize karton in the current directory")
def init():
    name = click.prompt("Name of your karton", type=str)
    path = click.prompt("Path to your karton package", type=str, default=os.getcwd())
    do_repository = click.confirm("Create git repository?")
    if do_repository:
        subprocess.check_call(["git", "init", path])

    karton_type = click.prompt(
        "Producer(p)/Consumer(c)/Karton(k)",
        type=click.Choice(["p", "c", "k"]),
        default="k",
    )
    comments = click.confirm(
        "Do you want to generate comments? (recommended for beginners)"
    )

    # Render name.py
    templates_mapping = {
        "k": "karton.jinja2",
        "p": "producer.jinja2",
        "c": "consumer.jinja2",
    }

    if karton_type not in templates_mapping:
        raise ValueError("This shouldn't happen, contact des")

    c = Config.from_default_path()

    render_vars = {
        "karton_name": name,
        "comments": comments,
        "prefix": c.dockerregistry_prefix,
    }

    # Generate karton.py/producer.py/consumer.py
    karton_filename = "{}.py".format(name.lower())
    template = templates_mapping[karton_type]
    render(template, karton_filename, render_vars)

    # Generate directory for deploy
    os.makedirs(os.path.join(path, "deploy/k8s"), exist_ok=True)
    os.makedirs(os.path.join(path, "deploy/docker"), exist_ok=True)

    # Render deployment files
    render(
        "deploy/deploy.json.jinja2",
        os.path.join(path, "deploy/deploy.json"),
        render_vars,
    )
    render(
        "deploy/k8s/deployment.yml.jinja2",
        os.path.join(path, "deploy/k8s/deployment.yml"),
        render_vars,
    )
    render(
        "deploy/docker/Dockerfile.jinja2",
        os.path.join(path, "deploy/docker/Dockerfile"),
        render_vars,
    )

    # Copy config.ini
    config_template_path = os.path.join(TEMPLATE_FOLDER, "config.ini.template")
    config_path = os.path.join(path, "config.ini")
    copyfile(config_template_path, config_path)

    # Create empty requirements.txt
    open(os.path.join(path, "requirements.txt"), "a").close()

    if do_repository:
        subprocess.check_call(["git", "-C", path, "add", "-A"])
        subprocess.check_call(
            ["git", "-C", path, "commit", "-m", "KPM initialized this repository."]
        )

    if c.use_kubernetes:
        subprocess.check_call(
            ["kubectl", "apply", "-f", os.path.join(path, "deploy/k8s")]
        )


def main():
    karton()


if __name__ == "__main__":
    main()
