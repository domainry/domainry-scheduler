package definitionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

type Store struct {
	mu       sync.Mutex
	current  map[string]metadatasdk.Definition
	versions map[string]metadatasdk.DefinitionVersion
}

func New() *Store {
	return &Store{current: map[string]metadatasdk.Definition{}, versions: map[string]metadatasdk.DefinitionVersion{}}
}

func (s *Store) Get(_ context.Context, owner, resourceType, resourceKey string) (metadatasdk.Definition, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, found := s.current[identity(owner, resourceType, resourceKey)]
	return clone(value), found, nil
}

func (s *Store) List(_ context.Context, query metadatasdk.DefinitionQuery) ([]metadatasdk.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := []metadatasdk.Definition{}
	for _, value := range s.current {
		if query.Owner != "" && value.Owner != query.Owner || query.ResourceType != "" && value.ResourceType != query.ResourceType || query.SourceID != "" && value.SourceID != query.SourceID {
			continue
		}
		values = append(values, clone(value))
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ResourceKey < values[j].ResourceKey })
	return values, nil
}

func (s *Store) Snapshot(ctx context.Context, query metadatasdk.DefinitionQuery) (metadatasdk.DefinitionSnapshot, error) {
	values, err := s.List(ctx, query)
	return metadatasdk.DefinitionSnapshot{Definitions: values}, err
}

func (s *Store) ReplaceSourceSnapshot(ctx context.Context, snapshot metadatasdk.ProjectionSnapshot) error {
	for _, definition := range snapshot.Definitions {
		definition.Owner = snapshot.Owner
		_, err := s.Publish(ctx, metadatasdk.DefinitionPublishCommand{
			Owner: snapshot.Owner, ResourceType: definition.ResourceType, ResourceKey: definition.ResourceKey,
			ExpectedCurrentVersionID: expectedVersion(s, snapshot.Owner, definition.ResourceType, definition.ResourceKey),
			SchemaVersion:            snapshot.SchemaVersion, SchemaHash: definition.SchemaHash, ObjectKey: definition.ObjectKey,
			Name: definition.Name, Payload: definition.Payload, SourceKind: snapshot.SourceKind, SourceID: snapshot.SourceID,
			PublishedBy: snapshot.SourceKind + ":" + snapshot.SourceID,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Publish(_ context.Context, command metadatasdk.DefinitionPublishCommand) (metadatasdk.DefinitionPublishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := identity(command.Owner, command.ResourceType, command.ResourceKey)
	current, found := s.current[key]
	if command.ExpectedCurrentVersionID == metadatasdk.DefinitionNoCurrentVersion {
		if found {
			return metadatasdk.DefinitionPublishResult{}, conflict()
		}
	} else if !found || current.CurrentVersionID != command.ExpectedCurrentVersionID {
		return metadatasdk.DefinitionPublishResult{}, conflict()
	}
	digest := sha256.Sum256(append([]byte(key+"\x00"), command.Payload...))
	versionID := "definition-version:" + hex.EncodeToString(digest[:16])
	if found && current.CurrentVersionID == versionID {
		if current.SchemaVersion == command.SchemaVersion && current.SchemaHash == command.SchemaHash && current.SourceKind == command.SourceKind && current.SourceID == command.SourceID && bytes.Equal(current.Payload, command.Payload) {
			return metadatasdk.DefinitionPublishResult{Definition: clone(current), CurrentVersionID: versionID}, nil
		}
		return metadatasdk.DefinitionPublishResult{}, conflict()
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	value := metadatasdk.Definition{
		Owner: command.Owner, ResourceType: command.ResourceType, ResourceKey: command.ResourceKey,
		CurrentVersionID: versionID, Status: "active", ObjectKey: command.ObjectKey, Name: command.Name,
		Payload: append([]byte(nil), command.Payload...), SchemaVersion: command.SchemaVersion, SchemaHash: command.SchemaHash,
		SourceKind: command.SourceKind, SourceID: command.SourceID, PublishedAt: now, PublishedBy: command.PublishedBy,
		CreatedAt: now, UpdatedAt: now,
	}
	if found {
		value.CreatedAt = current.CreatedAt
	}
	s.current[key] = value
	s.versions[versionID] = metadatasdk.DefinitionVersion{
		ID: versionID, Owner: command.Owner, ResourceType: command.ResourceType, ResourceKey: command.ResourceKey,
		SchemaVersion: command.SchemaVersion, SchemaHash: command.SchemaHash, Payload: append([]byte(nil), command.Payload...), CreatedAt: now,
	}
	return metadatasdk.DefinitionPublishResult{Definition: clone(value), CurrentVersionID: versionID}, nil
}

func (s *Store) Disable(_ context.Context, command metadatasdk.DefinitionDisableCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := identity(command.Owner, command.ResourceType, command.ResourceKey)
	current, found := s.current[key]
	if !found || current.CurrentVersionID != command.ExpectedCurrentVersionID {
		return conflict()
	}
	delete(s.current, key)
	return nil
}

func (s *Store) GetVersion(_ context.Context, query metadatasdk.DefinitionVersionQuery) (metadatasdk.DefinitionVersion, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if query.VersionID != "" {
		value, found := s.versions[query.VersionID]
		return value, found, nil
	}
	for _, value := range s.versions {
		if value.Owner == query.Owner && value.ResourceType == query.ResourceType && value.ResourceKey == query.ResourceKey && value.SchemaVersion == query.SchemaVersion {
			return value, true, nil
		}
	}
	return metadatasdk.DefinitionVersion{}, false, nil
}

func expectedVersion(s *Store, owner, resourceType, resourceKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, found := s.current[identity(owner, resourceType, resourceKey)]; found {
		return current.CurrentVersionID
	}
	return metadatasdk.DefinitionNoCurrentVersion
}

func identity(owner, resourceType, resourceKey string) string {
	return strings.Join([]string{strings.TrimSpace(owner), strings.TrimSpace(resourceType), strings.TrimSpace(resourceKey)}, "\x00")
}

func clone(value metadatasdk.Definition) metadatasdk.Definition {
	value.Payload = append([]byte(nil), value.Payload...)
	return value
}

func conflict() error {
	return &metadatasdk.Error{StatusCode: 409, Code: "metadata.definition_revision_conflict", Cause: fmt.Errorf("compare-and-swap lost")}
}

var _ metadatasdk.DefinitionStore = (*Store)(nil)
