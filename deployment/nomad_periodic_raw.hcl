job "ribeye_periodic" {

  type = "batch"

  periodic {
    cron             = "5 8 * * *"
    prohibit_overlap = true
  }


  task "ribeye_cook" {
    driver = "raw_exec"

    config {
      command = "/usr/local/bin/ribeye"
      args    = ["cook", "--dir", "/data/ribeye"]
    }

    resources {
      memory = 4000
    }
  }
}
