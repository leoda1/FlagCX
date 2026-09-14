import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class PpuCiRegressionTest(unittest.TestCase):
    def test_ppu_runner_executes_barex_heterogeneous_variants(self):
        source = (
            REPO_ROOT / ".github/scripts/set_env/ppu.sh"
        ).read_text()

        self.assertNotIn("Skipping PPU runner heterogeneous", source)
        self.assertIn("runner BAREX heterogeneous SendRecv smoke", source)
        self.assertIn("runner BAREX heterogeneous", source)
        self.assertIn("runner BAREX forced NET", source)
        self.assertGreaterEqual(source.count("FLAGCX_P2P_TRANSPORT=accl"), 3)
        self.assertNotIn("NCCL_P2P_DISABLE", source)
        self.assertNotIn("NCCL_SHM_DISABLE", source)
        self.assertEqual(source.count("FLAGCX_USE_HETERO_COMM=1"), 1)
        full_heterogeneous = source[
            source.index('FLAGCX_CI_MPI_LABEL="runner BAREX heterogeneous"'):
        ]
        self.assertNotIn("FLAGCX_USE_HETERO_COMM=1", full_heterogeneous)
        self.assertEqual(
            full_heterogeneous.count(
                "env -u FLAGCX_USE_HOST_COMM -u FLAGCX_USE_HETERO_COMM"
            ),
            2,
        )
        default_runner = source[source.index('FLAGCX_CI_MPI_LABEL="runner default"'):]
        default_runner = default_runner[:default_runner.index('FLAGCX_CI_MPI_LABEL="runner BAREX')]
        for variable in (
            "FLAGCX_USE_HETERO_COMM",
            "FLAGCX_CLUSTER_SPLIT_LIST",
            "FLAGCX_MEM_ENABLE",
            "FLAGCX_VMM_ENABLE",
            "FLAGCX_P2P_TRANSPORT",
            "FLAGCX_P2P_DISABLE",
        ):
            self.assertIn(f"-u {variable}", default_runner)

    def test_ppu_perf_uses_supported_collectives_for_each_runner(self):
        source = (
            REPO_ROOT / ".github/scripts/ci/run_ppu_workload.sh"
        ).read_text()
        homogeneous_operations = (
            "alltoall",
            "alltoallv",
            "sendrecv",
            "allreduce",
            "allgather",
            "reducescatter",
            "broadcast",
            "gather",
            "scatter",
            "reduce",
        )
        heterogeneous_operations = (
            "alltoall",
            "alltoallv",
            "sendrecv",
            "allgather",
            "broadcast",
            "gather",
            "scatter",
        )

        homogeneous_suite = source[source.index("homogeneous)\n"):]
        homogeneous_suite = homogeneous_suite[:homogeneous_suite.index(";;")]
        heterogeneous_suite = source[source.index("heterogeneous)\n"):]
        heterogeneous_suite = heterogeneous_suite[:heterogeneous_suite.index(";;")]
        for operation in homogeneous_operations:
            self.assertIn(operation, homogeneous_suite)
        for operation in heterogeneous_operations:
            self.assertIn(operation, heterogeneous_suite)
        for operation in ("allreduce", "reducescatter", "reduce"):
            self.assertNotIn(operation, heterogeneous_suite)
        self.assertIn("run_perf_suite homogeneous 128M 1G", source)
        self.assertIn("run_perf_suite heterogeneous 128M 1G", source)
        self.assertNotIn("run_perf heterogeneous sendrecv", source)
        self.assertIn("FLAGCX_USE_HETERO_COMM=1", source)
        self.assertIn("FLAGCX_CLUSTER_SPLIT_LIST=2", source)
        self.assertIn("FLAGCX_P2P_TRANSPORT=accl", source)
        self.assertNotIn("did not report ACCL/BAREX transport", source)
        self.assertNotIn("barex_log", source)
        self.assertNotIn("torch_log", source)

        perf_mode_env = source[source.index("  if [[ \"$mode\" == heterogeneous ]]"):]
        perf_mode_env = perf_mode_env[:perf_mode_env.index("  fi")]
        self.assertIn("FLAGCX_USE_HETERO_COMM=1", perf_mode_env)
        self.assertIn("FLAGCX_MEM_ENABLE=1", perf_mode_env)
        self.assertNotIn("FLAGCX_CLUSTER_SPLIT_LIST", perf_mode_env)

        clean_mode = source[source.index("local -a clean_mode_env=("):]
        clean_mode = clean_mode[:clean_mode.index("local -a mode_env=()")]
        for variable in (
            "FLAGCX_USE_HETERO_COMM",
            "FLAGCX_CLUSTER_SPLIT_LIST",
            "FLAGCX_MEM_ENABLE",
            "FLAGCX_VMM_ENABLE",
            "FLAGCX_P2P_TRANSPORT",
            "FLAGCX_P2P_DISABLE",
        ):
            self.assertIn(f"-u {variable}", clean_mode)

    def test_ppu_torch_heterogeneous_mode_uses_hybrid_runner(self):
        source = (
            REPO_ROOT / ".github/scripts/ci/run_ppu_workload.sh"
        ).read_text()
        torch_case = source[source.index("  torch-api)\n"):]
        torch_case = torch_case[:torch_case.index("    ;;")]

        self.assertIn("unset FLAGCX_USE_HETERO_COMM", torch_case)
        self.assertNotIn("export FLAGCX_USE_HETERO_COMM=1", torch_case)
        self.assertIn("export FLAGCX_CLUSTER_SPLIT_LIST=2", torch_case)
        self.assertIn("export FLAGCX_MEM_ENABLE=1", torch_case)
        self.assertIn("export FLAGCX_P2P_TRANSPORT=accl", torch_case)

    def test_ppu_jobs_are_present_in_public_workflows(self):
        perf_workflow = (
            REPO_ROOT / ".github/workflows/test.yml"
        ).read_text()
        torch_workflow = (
            REPO_ROOT / ".github/workflows/torch-api-test.yml"
        ).read_text()

        self.assertIn("perf-test-ppu:", perf_workflow)
        self.assertIn("name: perf-test (ppu)", perf_workflow)
        self.assertIn("run_ppu_container.sh\" perf", perf_workflow)
        self.assertIn("torch-api-test-ppu:", torch_workflow)
        self.assertIn("name: torch-api-test (ppu)", torch_workflow)
        self.assertIn("run_ppu_container.sh\" torch-api", torch_workflow)

        # BAREX needs host networking. GitHub Actions job containers reject
        # --network, so the PPU jobs must launch Docker explicitly.
        self.assertNotIn("container:", perf_workflow[perf_workflow.index("perf-test-ppu:"):])
        self.assertNotIn("container:", torch_workflow[torch_workflow.index("torch-api-test-ppu:"):])

        container_runner = (
            REPO_ROOT / ".github/scripts/ci/run_ppu_container.sh"
        ).read_text()
        self.assertIn("--network=host", container_runner)
        self.assertIn("docker run --rm", container_runner)

    def test_reference_platform_coverage_is_explicit(self):
        perf_workflow = (
            REPO_ROOT / ".github/workflows/test.yml"
        ).read_text()
        torch_workflow = (
            REPO_ROOT / ".github/workflows/torch-api-test.yml"
        ).read_text()

        hygon_perf = perf_workflow[perf_workflow.index("  perf-test-hygon:"):]
        hygon_perf = hygon_perf[:hygon_perf.index("  perf-test-cuda:")]
        metax_perf = perf_workflow[perf_workflow.index("  perf-test-metax:"):]
        metax_perf = metax_perf[:metax_perf.index("  perf-test-ppu:")]
        for platform_perf in (hygon_perf, metax_perf):
            self.assertIn("homogeneous 8-chip", platform_perf)
            for operation in (
                "alltoall",
                "alltoallv",
                "sendrecv",
                "allreduce",
                "allgather",
                "reducescatter",
                "broadcast",
                "gather",
                "scatter",
                "reduce",
            ):
                self.assertIn(f"perf_{operation}", platform_perf)

        self.assertIn("intentionally skips uniRunner", hygon_perf)
        self.assertNotIn("FLAGCX_USE_HETERO_COMM", hygon_perf)

        self.assertIn("Perf tests (MetaX uniRunner 8-chip)", metax_perf)
        metax_uni_runner = metax_perf[
            metax_perf.index("Perf tests (MetaX uniRunner 8-chip)"):
        ]
        self.assertEqual(metax_uni_runner.count("FLAGCX_USE_HETERO_COMM=1"), 7)
        self.assertEqual(metax_uni_runner.count("FLAGCX_MEM_ENABLE=1"), 7)
        for operation in (
            "alltoall",
            "alltoallv",
            "sendrecv",
            "allgather",
            "broadcast",
            "gather",
            "scatter",
        ):
            self.assertIn(f"perf_{operation}", metax_uni_runner)
        for operation in ("allreduce", "reducescatter", "reduce"):
            self.assertNotIn(f"perf_{operation}", metax_uni_runner)

        hygon_torch = torch_workflow[torch_workflow.index("  torch-api-test-hygon:"):]
        hygon_torch = hygon_torch[:hygon_torch.index("  torch-api-test-cuda:")]
        metax_torch = torch_workflow[torch_workflow.index("  torch-api-test-metax:"):]
        metax_torch = metax_torch[:metax_torch.index("  torch-api-test-ppu:")]
        self.assertIn('FLAGCX_SKIP_HETERO: "1"', hygon_torch)
        self.assertNotIn("FLAGCX_USE_HETERO_COMM", metax_torch)

    def test_perf_collectives_fail_fast_on_flagcx_errors(self):
        perf_dir = REPO_ROOT / "test/perf/host_api"
        operations = (
            "alltoall",
            "alltoallv",
            "sendrecv",
            "allreduce",
            "allgather",
            "reducescatter",
            "broadcast",
            "gather",
            "scatter",
            "reduce",
        )

        for operation in operations:
            source = (perf_dir / f"test_{operation}.cpp").read_text()
            self.assertIn("PERF_CHECK(flagcx", source, operation)


if __name__ == "__main__":
    unittest.main()
