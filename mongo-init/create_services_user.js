db.getSiblingDB("admin").createUser({
  user: "{SERVICES_USER}",
  pwd: "${SERVICES_PASSWORD}",
  roles: [
    { role: "readWrite", db: "BigData" },
    { role: "readWrite", db: "invertedIndex" }
  ]
})
