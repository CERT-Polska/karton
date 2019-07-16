#!/usr/bin/env python3
import click
import jinja2
import os
import sys
import json
import subprocess
from shutil import copyfile
from deploy import main as deploy_main

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
        os.makedirs(os.path.expanduser("~/.kpm"), exist_ok=True)
        prefix = click.prompt("Docker registry prefix", default="dr.cert.pl/karton")
        kubernetes = click.confirm("Do you have access to karton-prod(with kubectl)?")

        config = {"dockerregistry_prefix": prefix, "use_kubernetes": kubernetes}

        c = Config(config)
        c.save(CONFIG_PATH)


# cancer.
@karton.command(
    "deploy",
    short_help="builds and pushes karton image to repository",
    add_help_option=False,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
@click.pass_context
def deploy(ctx):
    c = Config.from_default_path()
    if not c.use_kubernetes:
        raise KubernetesNotEnabled()
    # ???
    sys.argv = ["kpm deploy"] + ctx.args
    deploy_main()


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

    render_environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(TEMPLATE_FOLDER)
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
    output_text = render_environment.get_template(
        templates_mapping[karton_type]
    ).render(render_vars)

    karton_filename = "{}.py".format(name.lower())
    with open(os.path.join(path, karton_filename), "w") as karton_file:
        karton_file.write(output_text)

    # Generate directory for deploy
    os.makedirs(os.path.join(path, "deploy/k8s"), exist_ok=True)
    os.makedirs(os.path.join(path, "deploy/docker"), exist_ok=True)

    # Render deploy.json
    output_text = render_environment.get_template("deploy/deploy.json.jinja2").render(
        render_vars
    )

    with open(os.path.join(path, "deploy/deploy.json"), "w") as deploy_file:
        deploy_file.write(output_text)

    # Render deployment.yml
    output_text = render_environment.get_template(
        "deploy/k8s/deployment.yml.jinja2"
    ).render(render_vars)

    with open(os.path.join(path, "deploy/k8s/deployment.yml"), "w") as deployment_file:
        deployment_file.write(output_text)

    # Render Dockerfile
    output_text = render_environment.get_template(
        "deploy/docker/Dockerfile.jinja2"
    ).render(render_vars)

    with open(os.path.join(path, "deploy/docker/Dockerfile"), "w") as docker_file:
        docker_file.write(output_text)

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
