resource "aws_glue_workflow" "workflow-jaf-pipeline" {
  name = "workflow-jaf-pipeline"
}


resource "aws_glue_trigger" "trigger_start" {
  name          = "gatilho"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.workflow-jaf-pipeline.name

  actions {
    crawler_name = aws_glue_crawler.jaf-crawler-pipeline
  }

}


resource "aws_glue_trigger" "trigger_aciona_job1" {
  name = "aciona-job1"
  type = "CONDITIONAL"

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.jaf-crawler-pipeline
      crawl_state  = "SUCCEEDED"
    }
  }
  
  actions {
    job_name = aws_glue_job.job1-jaf-pipeline
  }

}


resource "aws_glue_trigger" "trigger_aciona_job2" {
  name          = "aciona-job2"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.example.name

  predicate {
    conditions {
      job_name = aws_glue_job.job1-jaf-pipeline
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.job2-jaf-pipeline
  }

}


resource "aws_glue_trigger" "trigger_aciona_job3" {
  name          = "aciona-job3"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.example.name

  predicate {
    conditions {
      job_name = aws_glue_job.job2-jaf-pipeline
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.job3-jaf-pipeline
  }

}