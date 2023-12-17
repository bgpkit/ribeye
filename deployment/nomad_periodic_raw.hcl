job "ribeye_periodic" {

  type = "batch"

  periodic {
    crons            = ["5 8 * * *"]
    prohibit_overlap = true
  }


  task "ribeye_cook" {
    driver = "raw_exec"

    config {
      command = "/usr/local/bin/ribeye"
      args    = ["cook", "--dir", "s3://spaces/ribeye", "--env", "/usr/local/etc/bgpkit.d/ribeye_credentials"]
    }

    resources {
      memory = 4000
    }
  }
}
