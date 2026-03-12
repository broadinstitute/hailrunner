version 1.0

task hailrunner_run_task {
  input {
    String? project
    String staging_bucket
    String script
    Array[String] script_args = []
    Array[String] output_specs = []

    # Cluster config
    String region          = "us-central1"
    String subnet            = "subnetwork"
    Int    workers         = 16
    Int    preemptibles    = 0
    String worker_type     = "n1-highmem-8"
    String driver_type     = "n1-highmem-32"
    Int    worker_disk_gb  = 300
    Int    driver_disk_gb  = 500
    Int    max_idle        = 60
    Int    max_age         = 1440
    String? cluster_name
    String? service_account

    # Spark config
    Int    executor_cores  = 4
    String executor_memory = "26g"
    Int    driver_cores    = 4
    String driver_memory   = "26g"

    # WDL runtime
    String total_memory = "4GB"
    Int    disk_size_gb = 50
    Int    cpu          = 2
    String docker_image = "us-docker.pkg.dev/broad-dsde-methods/hailrunner/hailrunner:0.1.0"
  }

  Array[String] output_flags = prefix("--output ", output_specs)

  command {
    set -euo pipefail

    gcloud auth list

    python3 /usr/local/bin/hailrunner run \
      ~{"--project " + project} \
      --staging-bucket ~{staging_bucket} \
      --script ~{script} \
      --region ~{region} \
      --subnet ~{subnet} \
      --workers ~{workers} \
      --preemptibles ~{preemptibles} \
      --worker-type ~{worker_type} \
      --driver-type ~{driver_type} \
      --worker-disk-gb ~{worker_disk_gb} \
      --driver-disk-gb ~{driver_disk_gb} \
      --max-idle ~{max_idle} \
      --max-age ~{max_age} \
      ~{"--cluster-name " + cluster_name} \
      ~{"--service-account " + service_account} \
      --executor-cores ~{executor_cores} \
      --executor-memory ~{executor_memory} \
      --driver-cores ~{driver_cores} \
      --driver-memory ~{driver_memory} \
      ~{sep=" " output_flags} \
      ~{true="--" false="" length(script_args) > 0} ~{sep=" " script_args}
  }

  output {
    Array[File] output_files = glob("*")
  }

  runtime {
    memory: total_memory
    docker: docker_image
    cpu:    cpu
    disks:  "local-disk ~{disk_size_gb} SSD"
  }
}

workflow hailrunner_run {
  input {
    String? project
    String staging_bucket
    String script
    Array[String] script_args = []
    Array[String] output_specs = []

    String region          = "us-central1"
    String subnet            = "subnetwork"
    Int    workers         = 16
    Int    preemptibles    = 0
    String worker_type     = "n1-highmem-8"
    String driver_type     = "n1-highmem-32"
    Int    worker_disk_gb  = 300
    Int    driver_disk_gb  = 500
    Int    max_idle        = 60
    Int    max_age         = 1440
    String? cluster_name
    String? service_account

    Int    executor_cores  = 4
    String executor_memory = "26g"
    Int    driver_cores    = 4
    String driver_memory   = "26g"

    String total_memory = "4GB"
    Int    disk_size_gb = 50
    Int    cpu          = 2
    String docker_image = "us-docker.pkg.dev/broad-dsde-methods/hailrunner/hailrunner:0.1.0"
  }

  call hailrunner_run_task {
    input:
      project          = project,
      staging_bucket   = staging_bucket,
      script           = script,
      script_args      = script_args,
      output_specs     = output_specs,
      region           = region,
      subnet           = subnet,
      workers          = workers,
      preemptibles     = preemptibles,
      worker_type      = worker_type,
      driver_type      = driver_type,
      worker_disk_gb   = worker_disk_gb,
      driver_disk_gb   = driver_disk_gb,
      max_idle         = max_idle,
      max_age          = max_age,
      cluster_name     = cluster_name,
      service_account  = service_account,
      executor_cores   = executor_cores,
      executor_memory  = executor_memory,
      driver_cores     = driver_cores,
      driver_memory    = driver_memory,
      total_memory     = total_memory,
      disk_size_gb     = disk_size_gb,
      cpu              = cpu,
      docker_image     = docker_image
  }

  output {
    Array[File] output_files = hailrunner_run_task.output_files
  }
}
