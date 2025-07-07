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

type OpenAIClient struct {
	client               *openai.Client
	assistantID          string
	productsJSONFileName string
}

// initialise and returns a new OpenAIClient.
func NewOpenAIClient() (*OpenAIClient, error) {
	assistantID := os.Getenv("ASSISTANT_PRODUCT_PICKER")
	if assistantID == "" {
		return nil, fmt.Errorf("ASSISTANT_PRODUCT_PICKER environment variable not set")
	}

	openAICredential := os.Getenv("OPEN_AI_CREDENTIAL")
	if openAICredential == "" {
		return nil, fmt.Errorf("OPEN_AI_CREDENTIAL environment variable not set")
	}

	productsJSONFileName := os.Getenv("PRODUCTS_FILE_NAME")
	if productsJSONFileName == "" {
		return nil, fmt.Errorf("PRODUCTS_FILE_NAME environment variable not set")
	}

	// Configure the client to use Assistants API v2
	config := openai.DefaultConfig(openAICredential)
	config.AssistantVersion = "v2"

	client := openai.NewClientWithConfig(config)

	return &OpenAIClient{
		client:               client,
		assistantID:          assistantID,
		productsJSONFileName: productsJSONFileName,
	}, nil
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
	ctx := context.TODO()

	oc, err := NewOpenAIClient()
	if err != nil {
		return fmt.Errorf("failed to initialize OpenAI client: %w", err)
	}

	vectorStoreID, err := oc.getAssistantVectorStoreID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get or create assistant's vector store: %w", err)
	}

	existingFiles, err := oc.listFilesInVectorStore(ctx, vectorStoreID)
	if err != nil {
		return fmt.Errorf("failed to list files in vector store: %w", err)
	}

	var fileToDeleteID string
	for _, file := range existingFiles {

		fileDetails, fileErr := oc.client.GetFile(ctx, file.ID) // Direct call on client
		if fileErr != nil {
			log.Printf("Warning: Could not retrieve details for file %s in vector store: %v", file.ID, fileErr)
			continue // Skip this file if details can't be retrieved
		}

		if fileDetails.FileName == oc.productsJSONFileName {
			fileToDeleteID = file.ID
			log.Printf("Found existing file '%s' with File ID: %s in Vector Store. Detaching and deleting...\n", oc.productsJSONFileName, fileToDeleteID)
			if err := oc.deleteFileFromOpenAIAndVectorStore(ctx, vectorStoreID, fileToDeleteID); err != nil {
				return fmt.Errorf("failed to delete old file from vector store and storage: %w", err)
			}
			break
		}
	}

	if fileToDeleteID == "" {
		log.Printf("No existing file '%s' found in Vector Store. Proceeding with upload.\n", oc.productsJSONFileName)
	}

	fileId, err := oc.uploadFileToOpenAIAndVectorStore(ctx, vectorStoreID, JSONBytes)
	if err != nil {
		return fmt.Errorf("failed to upload and attach new products file: %w", err)
	}

	updatedFiles, err := oc.listFilesInVectorStore(ctx, vectorStoreID)
	if err != nil {
		return fmt.Errorf("error listing vector store files after upload: %w", err)
	}

	JSONTing, err := json.Marshal(updatedFiles)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON output: %v", err)
	}

	log.Print(string(JSONTing))
	found := false
	for _, file := range updatedFiles {
		if file.ID == fileId {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("file uploaded but attaching to vector store failed or verification issue")
	}

	log.Println("Successfully replaced products JSON file in OpenAI via Vector Store.")
	return nil
}

// get id of Vector Store for assistant
// ASSUMES one already exists
func (oc *OpenAIClient) getAssistantVectorStoreID(ctx context.Context) (string, error) {
	assistant, err := oc.client.RetrieveAssistant(ctx, oc.assistantID)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve assistant %s: %w", oc.assistantID, err)
	}

	// vector storage stuff is found within the nested ToolResources field of the Assistant struct.
	// These are accessed directly as fields of the `assistant` object.
	if assistant.ToolResources != nil &&
		assistant.ToolResources.FileSearch != nil &&
		len(assistant.ToolResources.FileSearch.VectorStoreIDs) > 0 {
		vectorStoreID := assistant.ToolResources.FileSearch.VectorStoreIDs[0]
		log.Printf("Found existing Vector Store ID: %s associated with assistant.", vectorStoreID)
		return vectorStoreID, nil
	}
	return "", nil
}

func (oc *OpenAIClient) listFilesInVectorStore(ctx context.Context, vectorStoreID string) ([]openai.VectorStoreFile, error) {

	limit := 1 // should only be one thing in there
	var cursor *string
	orderBy := "desc"
	filesList, err := oc.client.ListVectorStoreFiles(ctx, vectorStoreID, openai.Pagination{
		Limit: &limit,
		After: cursor,
		Order: &orderBy,
	})
	if err != nil {
		return nil, fmt.Errorf("error listing files in Vector Store %s: %w", vectorStoreID, err)
	}
	return filesList.VectorStoreFiles, nil
}

func (oc *OpenAIClient) deleteFileFromOpenAIAndVectorStore(ctx context.Context, vectorStoreID, fileID string) error {
	err := oc.client.DeleteVectorStoreFile(ctx, vectorStoreID, fileID)
	if err != nil {
		return fmt.Errorf("error deleting file %s from Vector Store %s: %w", fileID, vectorStoreID, err)
	}

	err = oc.client.DeleteFile(ctx, fileID)
	if err != nil {
		return fmt.Errorf("error deleting file %s from OpenAI storage: %w", fileID, err)
	}
	return nil
}

func (oc *OpenAIClient) uploadFileToOpenAIAndVectorStore(ctx context.Context, vectorStoreID string, JSONBytes []byte) (string, error) {
	uploadReq := openai.FileBytesRequest{
		Name:    oc.productsJSONFileName,
		Bytes:   JSONBytes,
		Purpose: purpose,
	}

	uploadedFile, err := oc.client.CreateFileBytes(ctx, uploadReq)
	if err != nil {
		return "", fmt.Errorf("error uploading file to OpenAI storage: %w", err)
	}
	attachReq := openai.VectorStoreFileRequest{
		FileID: uploadedFile.ID,
	}
	_, err = oc.client.CreateVectorStoreFile(ctx, vectorStoreID, attachReq)
	if err != nil {
		return "", fmt.Errorf("error attaching file %s to Vector Store %s: %w", uploadedFile.ID, vectorStoreID, err)
	}
	return uploadedFile.ID, nil
}
