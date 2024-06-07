import * as dotenv from "dotenv";
import * as AWS from "aws-sdk";
import { QueryInput, ScanInput } from "aws-sdk/clients/dynamodb";
import { createClient, SupabaseClient } from "@supabase/supabase-js";

dotenv.config();

AWS.config.update({
  region: process.env.DDB_REGION,
  accessKeyId: process.env.DDB_ACCESS_KEY,
  secretAccessKey: process.env.DDB_SECRET_KEY,
});

const docClient = new AWS.DynamoDB.DocumentClient({ apiVersion: "2012-08-10" });
const s3Client = new AWS.S3({ apiVersion: "2012-08-10" });
const supabaseClient: SupabaseClient = createClient(
  process.env.SUPABASE_URI,
  process.env.SUPABASE_KEY,
  {
    global: {
      headers: {
        Authorization:
          "Bearer YOUR SUPABASE TOKEN"
      },
    },
  }
);

const table = "Product";

const tableName = `${table}-4i6eliuey5bphp7uom3vuz4bh4-dev`;

function camelToSnake(
  name: string | undefined | null
): string | undefined | null {
  try {
    if (name == null || typeof name === "number") {
      return name;
    }
    return name.replace(/([a-z])([A-Z])/g, "$1_$2").toLowerCase();
  } catch (err) {
    console.log(typeof name);
  }
}

function isDateValid(dateStr: string): boolean {
  return !isNaN(new Date(dateStr) as any);
}

const parseFields = (object: any) => {
  const updatedObject = Object.entries(object).map((val) => {
    const key = val[0];
    let value = val[1] as any;
    value = isDateValid(value) ? new Date(value) : value;

    return [key, value];
  });

  return Object.fromEntries(updatedObject);
};

const fetchAndParseTable = async (
  table: string,
  id?: string
): Promise<any[] | null> => {
  let params: ScanInput | QueryInput = {
    TableName: table,
  };

  if (id) {
    params = {
      TableName: table,
      KeyConditionExpression: "#id = :id",
      ExpressionAttributeNames: {
        "#id": "id",
      },
      ExpressionAttributeValues: {
        ":id": id,
      },
    } as QueryInput;
  }

  try {
    let data: any;
    let items = [];

    if (id) {
      data = await docClient.query(params).promise();
      items = data.Items;
    } else {
      do {
        data = await docClient.scan(params).promise();
        items = items.concat(data.Items);
        (params as any).ExclusiveStartKey = data.LastEvaluatedKey;
      } while (typeof data.LastEvaluatedKey !== "undefined");
    }

    items = items.map((item) => {
      const { __typename, owner, stickerSize, ...updatedItem } = item;
      let objects = Object.entries(updatedItem);
      objects = objects.map((object) => [camelToSnake(object[0]), object[1]]);
      const parsedItem = Object.fromEntries(objects);
      return parsedItem;
    });

    return items;
  } catch (err) {
    console.error(
      "Unable to scan or query the table. Error JSON:",
      JSON.stringify(err, null, 2)
    );
    return null;
  }
};

const migrateGeneralDatabase = async () => {
  const productCartonData = await fetchAndParseTable(tableName);
  console.log(productCartonData);
  try {
    const { error } = await supabaseClient
      .from(table)
      .insert(productCartonData);
    if (error) {
      console.error(error);
    } else {
      console.log(`Successfully updated data to ${table}`);
    }
  } catch (err) {
    console.log(err);
  }
};

const parseTemplateJson = async (template_json) => {
  const tableName = "ProductSpecification-4i6eliuey5bphp7uom3vuz4bh4-dev";

  if (!template_json) return template_json;
  const fetchAndUpdate = async (id) => {
    try {
      const items = await fetchAndParseTable(tableName, id);
      return items && items.length > 0 ? items[0] : null;
    } catch (error) {
      console.error("Error fetching data for specificationId:", id, error);
      return null;
    }
  };

  const fixedCamelCase = await Promise.all(
    Object.entries(template_json).map(async ([_, value]) => {
      const parsedObject = await Promise.all(
        Object.entries(value).map(async ([key, val]) => {
          if (key === "__typename") {
            return undefined;
          } else if (key === "specificationId") {
            const fetchedItem = await fetchAndUpdate(val);
            return fetchedItem ? ["key", fetchedItem.key.trim()] : undefined;
          } else {
            if (val != undefined) return [key, camelToSnake(val)];
          }
        })
      );

      return Object.fromEntries(
        parsedObject.filter((entry) => entry !== undefined)
      );
    })
  );

  return fixedCamelCase;
};

const migrateProduct = async () => {
  try {
    const items = await fetchAndParseTable(tableName);
    const stickerItems = await fetchAndParseTable(
      "ProductSticker-4i6eliuey5bphp7uom3vuz4bh4-dev"
    );
    const specificationItems = await fetchAndParseTable(
      "ProductSpecification-4i6eliuey5bphp7uom3vuz4bh4-dev"
    );

    const itemsWithStickerAndSpecification = await Promise.all(
      items.map(async (item) => {
        const currentStickerItem = stickerItems.find(
          (stickerItem) => item.id === stickerItem.id
        );

        const currentSpecificationItem = specificationItems
          .filter(
            (specificationItem) => item.id === specificationItem.product_id
          )
          .reduce((acc, val) => {
            acc[String(val.key).trim()] = String(val.value).trim();
            return acc;
          }, {});

        const updatedItem = {
          ...item,
          template_json: await parseTemplateJson(
            currentStickerItem?.template_json
          ),
          template_html: currentStickerItem?.template_html,
          product_specification: currentSpecificationItem,
        };

        return updatedItem;
      })
    );

    try {
      const { error } = await supabaseClient
        .from(table.toLowerCase())
        .upsert(itemsWithStickerAndSpecification);
      if (error) {
        console.error(error);
      } else {
        console.log("Successfully uploaded data");
      }
    } catch (err) {
      console.error(
        "Error uploading to supabase. JSON:",
        JSON.stringify(err, null, 2)
      );

      throw new Error(err);
    }
  } catch (err) {
    console.log(err);
    console.error(
      "Unable to scan the table. Error JSON:",
      JSON.stringify(err, null, 2)
    );
  }
};

// Storage migration
const getImageFromS3 = async () => {
  var keys = [];
  s3Client.listObjectsV2(
    {
      Bucket: "powertoolsfcd7fb1fd52141e8aa833e56d134581c111239-dev",
    },
    (err, data) => {
      data.Contents.map((e) => e.Key.length > 15 && keys.push(e.Key));
      const imageLinks = keys.map((val) => {
        const url = s3Client.getSignedUrl("getObject", {
          Bucket: "powertoolsfcd7fb1fd52141e8aa833e56d134581c111239-dev",
          Key: val,
          Expires: 60 * 5,
        });
        return {
          name: String(val).replace("public/", ""),
          url,
        };
      });
      downloadImages(imageLinks);
    }
  );
};

const downloadImages = async (links: any[]) => {
  let count = 0;
  for (var val of links) {
    const name = val["name"];
    const res = await fetch(val["url"], {
      mode: "cors",
    });

    const image = await res.blob();
    const arrayBuffer = await image.arrayBuffer();

    const { data, error } = await supabaseClient.storage
      .from("images")
      .upload(name, arrayBuffer, {
        cacheControl: "3600",
        upsert: true,
        contentType:
          "image/" +
          (name.split(".")[1].toLowerCase() == "jpg"
            ? "jpeg"
            : name.split(".")[1].toLowerCase()),
      });

    console.log(
      data,
      error,
      "image/" +
        (name.split(".")[1].toLowerCase() == "jpg"
          ? "jpeg"
          : name.split(".")[1].toLowerCase())
    );

    console.log(`${++count}/${links.length}`);
  }
};

migrateProduct();
