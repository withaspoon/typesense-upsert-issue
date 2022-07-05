import fs from "fs-extra";
import ndjson from "ndjson";
import Typesense from "typesense";
import {
  CollectionFieldSchema,
  FieldType,
} from "typesense/lib/Typesense/Collection";

const client = new Typesense.Client({
  nodes: [
    {
      host: "127.0.0.1",
      port: 8108,
      protocol: "http",
    },
  ],
  apiKey: "xyz",
  numRetries: 3,
  connectionTimeoutSeconds: 10,
  logLevel: "debug",
});

const docs: any[] = [];

fs.createReadStream("data.ndjson")
  .pipe(ndjson.parse())
  .on("data", (doc) => docs.push(doc))
  .on("end", migrate);

function createField(
  name: string,
  type: FieldType,
  optional: boolean
): CollectionFieldSchema {
  return {
    name,
    type,
    facet: false,
    optional,
    index: true,
    sort: true,
  };
}

async function migrate() {
  const existingCollections = await client.collections().retrieve();
  const collectionNames = new Set(docs.map((doc) => doc.type));
  collectionNames.forEach(async (name) => {
    const fields = [
      createField("_createdAt", "int64", false),
      createField("dataset", "string", true),
      createField("type", "string", true),
    ];

    if (!existingCollections.find((c) => c.name === name)) {
      await client
        .collections()
        .create({ name, fields, default_sorting_field: "_createdAt" });
    }

    const upserts = docs
      .filter((doc) => doc.type === name)
      .map(({ type, id, dataset, _createdAt }) => ({
        type,
        id,
        dataset,
        _createdAt,
      }));

    console.log(upserts);

    await client.collections(name).documents().import(upserts, {
      action: "upsert",
      dirty_values: "coerce_or_drop",
    });
  });
}
