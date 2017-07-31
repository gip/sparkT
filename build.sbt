
lazy val root = (project in file("."))
  .aggregate(ast, backend)
  .settings(
    name := "root"
  )

lazy val ast = (project in file("sparkt-ast-scala"))
  .settings(
    name := "ast"
  )
lazy val backend = (project in file("sparkt-be-spark"))
  .dependsOn(ast)
  .settings(
    name := "backend"
  )
