"""Stub for future k3s-based EKS cluster backend.

When ROBOTOCORE_EKS_ENGINE=k3s is set, this module would spin up real k3s
instances instead of mock K8s API servers. Currently a placeholder.
"""

import logging
import shutil

logger = logging.getLogger(__name__)


class K3sManager:
    """Manages k3s instances for EKS clusters (stub implementation)."""

    @staticmethod
    def is_available() -> bool:
        """Check whether the k3s binary is on PATH."""
        return shutil.which("k3s") is not None

    def start(self, cluster_name: str) -> tuple[int, str]:
        """Start a k3s instance for the given cluster.

        Returns:
            (port, kubeconfig_path) tuple. Currently logs a warning and
            returns dummy values.
        """
        logger.warning(
            "k3s engine not implemented; using mock K8s server for cluster %s",
            cluster_name,
        )
        return 0, ""

    def stop(self, cluster_name: str) -> None:
        """Stop the k3s instance for the given cluster (stub)."""
        logger.warning(
            "k3s engine not implemented; nothing to stop for cluster %s",
            cluster_name,
        )
