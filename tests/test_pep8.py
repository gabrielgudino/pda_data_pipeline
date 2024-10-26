import subprocess


def test_pep8_conformance():
    """
    Test que asegura que el código cumple con PEP8, excluyendo algunos directorios.
    """
    result = subprocess.run(['flake8', '--max-line-length=88',
                             '--exclude=pdap,drafts,pdap_test',
                             './'], capture_output=True, text=True)
    assert result.returncode == 0, f"Errores de PEP8:\n{result.stdout}"
