package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	openai "github.com/sashabaranov/go-openai"
)

const (
	filePath = "products.json"
	purpose  = openai.PurposeAssistants
)

// each dynamodb record top level structure
type Product struct {
	ProductID  string                 `dynamodbav:"product_id"`
	Category   string                 `dynamodbav:"category"`
	Attributes map[string]interface{} `dynamodbav:"attributes"` // interface{} = any type
}

// each individual product_id:attribute mapping inside each attribute in attributes_list
type AttributeValues map[string]string

// each attribute section inside attributes_list
type AttributesList map[string]AttributeValues

// each category with its products listed within the attributes
type OutputCategory struct {
	Name           string         `json:"name"`
	AttributesList AttributesList `json:"attributes_list"`
}

func main() {
	lambda.Start(handleRequest)
}

func handleRequest() error {
	products, err := getProducts()
	if err != nil {
		return fmt.Errorf("failed to get products: %v", err)
	}
	reshuffled := convertProductFormat(products)
	JSONScarramuzza, err := json.Marshal(reshuffled)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON output: %v", err)
	}

	log.Println(string(JSONScarramuzza))

	err = replaceProductsJSONFileInOpenAI(JSONScarramuzza)

	if err != nil {
		return fmt.Errorf("failed to replace products json file on open ai: %v", err)

	}

	return nil
}

func getProducts() (*dynamodb.ScanOutput, error) {
	tableName := os.Getenv("DYNAMODB_PRODUCTS_TABLE")
	awsRegion := "eu-west-1"

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion))
	if err != nil {
		log.Fatalf("unable to load SDK config: %v", err)
	}

	dynamoTING := dynamodb.NewFromConfig(cfg)
	result, err := dynamoTING.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.Fatalf("failed to scan table %s: %v", tableName, err)
		return nil, err
	}
	return result, nil
}

func convertProductFormat(products *dynamodb.ScanOutput) []OutputCategory {

	// split the products out into the categories
	categorisedProducts := make(map[string][]Product)
	for _, item := range products.Items {
		var product Product
		err := attributevalue.UnmarshalMap(item, &product)
		if err != nil {
			log.Printf("failed to unmarshal item: %v", err)
			continue
		}
		categorisedProducts[product.Category] = append(categorisedProducts[product.Category], product)
	}

	var reJigged []OutputCategory
	for categoryName, products := range categorisedProducts {
		categoryOutput := OutputCategory{
			Name:           categoryName,
			AttributesList: make(AttributesList),
		}

		// All attributes are strictly consistent at an earlier point,
		// so assume the first element is representative of a consistent
		// structure
		if len(products) > 0 && products[0].Attributes != nil {
			for attrName := range products[0].Attributes {
				categoryOutput.AttributesList[attrName] = make(AttributeValues)
			}
		}

		for _, product := range products {
			for attrName, attrValue := range product.Attributes {
				categoryOutput.AttributesList[attrName][product.ProductID] = fmt.Sprintf("%v", attrValue)
			}
		}
		reJigged = append(reJigged, categoryOutput)
	}

	return reJigged
}

func replaceProductsJSONFileInOpenAI(JSONBytes []byte) error {
	assistantID := os.Getenv("ASSISTANT_PRODUCT_PICKER")

	if assistantID == "" {
		return fmt.Errorf("ASSISTANT_PRODUCT_PICKER environment variable not set")
	}

	openAICredential := os.Getenv("OPEN_AI_CREDENTIAL")
	if openAICredential == "" {
		return fmt.Errorf("OPEN_AI_CREDENTIAL environment variable not set")
	}

	productsJSONFileName := os.Getenv("PRODUCTS_FILE_NAME")
	if productsJSONFileName == "" {
		return fmt.Errorf("PRODUCTS_FILE_NAME environment variable not set")
	}

	config := openai.DefaultConfig(openAICredential)
	config.AssistantVersion = "v2"
	config.APIVersion = "v2"
	client := openai.NewClientWithConfig(config)
	var fileToDeleteID string

	// @TODO: "failed to replace products json file on open ai: error listing assistant files: error, status code: 400, status: 400 Bad Request, message: The v1 Assistants API has been deprecated. See the migration guide for more information: https://platform.openai.com/docs/assistants/migration."
	// consider alternatives and that innit
	files, err := client.ListAssistantFiles(context.TODO(), assistantID, nil, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("error listing assistant files: %w", err)
	}

	for _, file := range files.AssistantFiles {
		if file.Object == productsJSONFileName {
			fileToDeleteID = file.ID
			log.Printf("Found existing file '%s' with File ID: %s attached to assistant. Detaching and deleting...\n", productsJSONFileName, fileToDeleteID)

			// Detach the file from the assistant first
			err := client.DeleteAssistantFile(context.TODO(), assistantID, fileToDeleteID)
			if err != nil {
				return fmt.Errorf("error deleting file %s from assistant %s: %w", fileToDeleteID, assistantID, err)
			}
			log.Printf("Successfully detached file %s from assistant.\n", fileToDeleteID)

			// Then delete the file from OpenAI's general file storage
			err = client.DeleteFile(context.TODO(), fileToDeleteID)
			if err != nil {
				return fmt.Errorf("error deleting file %s from OpenAI storage: %w", fileToDeleteID, err)
			}
			log.Printf("Successfully deleted file %s from OpenAI storage.\n", fileToDeleteID)
			break
		}
	}

	if fileToDeleteID == "" {
		log.Printf("No existing file '%s' found. Still finna upload tho.\n", productsJSONFileName)
	}

	uploadReq := openai.FileBytesRequest{
		Name:    productsJSONFileName,
		Bytes:   JSONBytes,
		Purpose: purpose,
	}

	uploadedFile, err := client.CreateFileBytes(context.TODO(), uploadReq)
	if err != nil {
		return fmt.Errorf("error uploading file: %w", err)
	}

	// attach new file to assistant
	attachReq := openai.AssistantFileRequest{
		FileID: uploadedFile.ID,
	}
	_, err = client.CreateAssistantFile(context.TODO(), assistantID, attachReq)
	if err != nil {
		return fmt.Errorf("error attaching file %s to assistant %s: %w", uploadedFile.ID, assistantID, err)
	}

	// verify file is attached to assistant
	updatedAssistantFiles, err := client.ListAssistantFiles(context.TODO(), assistantID, nil, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("error listing assistant files after attachment: %w", err)
	}
	found := false
	for _, file := range updatedAssistantFiles.AssistantFiles {
		if file.Object == productsJSONFileName {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("file uploaded but attaching to assistant failed")
	}

	return nil
}
